# pg_ai_query Middleware — Architecture

## Overview

The current pg_ai_query architecture embeds all LLM logic inside the C extension.
This makes it impossible to update prompting strategies, swap models, or configure
API keys without recompiling PostgreSQL. This middleware proposal decouples all AI
logic into a standalone Python daemon that communicates with the C extension over
a Unix socket.

---

## IPC Flow

```
┌─────────────────────────────────────────────────────────────────┐
│  PostgreSQL Process                                             │
│                                                                 │
│  ┌──────────────────────────────────────────────┐              │
│  │  pg_ai_query C Extension                     │              │
│  │                                              │              │
│  │  pg_ai_query('show all users')  ─────────────┼──────────┐  │
│  │                                              │          │  │
│  │  ← returns validated SQL ───────────────────┼──────────┘  │
│  └──────────────────────────────────────────────┘              │
└──────────────────────────────┬──────────────────────────────────┘
                               │  Unix Socket
                               │  /tmp/pg_ai_query.sock
                               │  length-prefixed JSON frames
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  pg_ai_worker.py  (Python daemon, runs alongside PostgreSQL)    │
│                                                                 │
│  1. Receives: {"question": "show all users"}                    │
│                                                                 │
│  2. Schema introspection ──────────────────────────────────┐   │
│     SELECT table_name, columns FROM information_schema     │   │
│     SELECT extname FROM pg_extension                       │   │
│     SELECT current_setting('server_version_num')          │   │
│     → builds schema_context string                         │   │
│                                                            │   │
│  3. Context7 MCP (optional) ───────────────────────────┐  │   │
│     POST https://mcp.context7.com/mcp                   │  │   │
│     → fetches relevant PostgreSQL docs for the query    │  │   │
│                                                         │  │   │
│  4. LLM call ───────────────────────────────────────┐  │  │   │
│     system_prompt = schema_context + docs_context   │  │  │   │
│     user_message  = natural language question       │  │  │   │
│     → model returns raw SQL                         │  │  │   │
│                                                     │  │  │   │
│  5. Validation loop (up to 3 retries) ──────────────┘  │  │   │
│     extract_sql() strips markdown fences               │  │   │
│     DANGEROUS_PATTERNS blocks write/DDL ops            │  │   │
│     EXPLAIN {sql} validates syntax via PostgreSQL      │  │   │
│     if invalid → feed error back to LLM, retry        │  │   │
│                                                        │  │   │
│  6. Returns: {"sql": "SELECT ...", "error": null}  ◄───┘  │   │
│                                                            │   │
└────────────────────────────────────────────────────────────────┘
                               │
              ┌────────────────┼──────────────────┐
              ▼                ▼                   ▼
     ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐
     │  OpenAI API  │  │ Anthropic API│  │  Ollama (local)  │
     │  gpt-4o      │  │  claude-*    │  │  llama3, etc.    │
     └──────────────┘  └──────────────┘  └──────────────────┘
```

---

## Socket Protocol

Communication between the C extension and the Python daemon uses
**length-prefixed JSON frames** over a Unix domain socket.

### Request (C extension → daemon)

```
[ 4 bytes: uint32 big-endian payload length ][ N bytes: UTF-8 JSON ]

JSON shape:
{
  "question": "show all users sorted by signup date"
}
```

### Response (daemon → C extension)

```
[ 4 bytes: uint32 big-endian payload length ][ N bytes: UTF-8 JSON ]

JSON shape (success):
{
  "sql":   "SELECT * FROM users ORDER BY created_at DESC;",
  "error": null
}

JSON shape (failure after max retries):
{
  "sql":   null,
  "error": "Failed to generate valid SQL after 3 attempts. Last error: ..."
}
```

---

## Configuration

All configuration is read by the Python daemon from environment variables
(or a `.env` file). The C extension has zero configuration coupling —
it only knows the socket path.

| Variable | Default | Description |
|---|---|---|
| `PG_AI_SOCKET` | `/tmp/pg_ai_query.sock` | Unix socket path |
| `PG_AI_DSN` | `postgresql://localhost/postgres` | Database to introspect |
| `PG_AI_PROVIDER` | `openai` | `openai` / `anthropic` / `ollama` |
| `PG_AI_MODEL` | `gpt-4o` | Model name |
| `PG_AI_API_KEY` | *(required)* | API key — never touches the C extension |
| `PG_AI_CONTEXT7` | `true` | Enable live PostgreSQL docs fetching |

---

## Validation Pipeline Detail

```
Raw LLM output
      │
      ▼
extract_sql()          ← strips ```sql fences, trims whitespace
      │
      ▼
DANGEROUS_PATTERNS     ← regex blocks DROP/DELETE/INSERT/UPDATE/ALTER/CREATE/TRUNCATE
      │ blocked → return error immediately (no retry)
      │ allowed ↓
      ▼
EXPLAIN {sql}          ← sent to PostgreSQL via asyncpg
      │ syntax error → capture error message
      │              → feed back to LLM: "Your previous query failed with: ..."
      │              → retry (up to max_retries=3)
      │ valid ↓
      ▼
Return SQL to C extension
```

---

## Benchmark Design

The accuracy benchmark (`benchmark.py`) measures what fraction of
natural-language → SQL conversions are syntactically valid after the
full pipeline runs.

| Category | Count | Tests |
|---|---|---|
| Simple | 30 | SELECT with WHERE, ORDER BY, basic aggregation |
| Intermediate | 30 | JOINs, subqueries, GROUP BY + HAVING |
| Advanced | 20 | Window functions, CTEs, recursive queries |
| PostgreSQL-specific | 10 | JSONB operators, array functions, full-text search |
| Version-sensitive | 10 | MERGE (PG15+), generated columns (PG12+), etc. |

**Target improvement:** ≥40% accuracy gain on the PostgreSQL-specific
and version-sensitive categories compared to the baseline (no schema
context, no docs context).