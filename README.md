# pg_ai_query — Middleware Daemon (GSoC 2026 Proposal)

This is the prototype middleware for the [pg_ai_query GSoC 2026 proposal](https://wiki.postgresql.org/wiki/GSoC_2026).
It decouples all AI logic from the PostgreSQL C extension into a standalone Python daemon.

## Architecture

```
C Extension  <──(Unix socket)──>  pg_ai_worker.py  <──(HTTP)──>  LLM API
                                        │
                                   PostgreSQL (schema introspection)
                                   Context7 MCP (live docs)
```

## Requirements

- Python 3.11+
- PostgreSQL 14–17 running locally
- An OpenAI, Anthropic, or local Ollama API key

## Installation

```bash
git clone https://github.com/<your-handle>/pg_ai_query_middleware
cd pg_ai_query_middleware

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

pip install -r requirements.txt

cp .env.example .env
# Edit .env and fill in PG_AI_DSN and PG_AI_API_KEY
```

## Running the daemon

```bash
python pg_ai_worker.py
# Listens on /tmp/pg_ai_query.sock by default
```

Override the socket path or DSN:

```bash
PG_AI_SOCKET=/var/run/pg_ai.sock PG_AI_DSN=postgresql://localhost/mydb python pg_ai_worker.py
```

## Running the accuracy benchmark

```bash
# Make sure pg_ai_worker.py is running in another terminal first
python benchmark.py --dsn postgresql://localhost/mydb --socket /tmp/pg_ai_query.sock
```

Expected output:

```
Running benchmark against PostgreSQL 16
Socket: /tmp/pg_ai_query.sock
Cases: 14

  ✓  [S01] Show all customers (312ms)
  ✓  [S02] How many orders are there? (287ms)
  ...

============================================================
OVERALL: 12/14 (86%) syntactically valid

By category:
  advanced             2/3 (67%)
  intermediate         3/3 (100%)
  pg_specific          2/3 (67%)
  simple               4/4 (100%)
  version_sensitive    1/1 (100%)

Average latency: 304ms per query
```

## Running tests

```bash
pip install pytest pytest-asyncio
pytest tests/ -v
```

## Using with Ollama (no API key needed)

```bash
# Install Ollama: https://ollama.com
ollama pull llama3

# In .env:
PG_AI_PROVIDER=ollama
PG_AI_MODEL=llama3
# PG_AI_API_KEY can be left blank
```

## Project structure

```
postgresql-proposal1/
├── pg_ai_query/
│ ├── pg_ai_worker.py # Main middleware daemon (LLM + socket handling)
│ ├── benchmark.py # Evaluation / accuracy testing
│ ├── requirements.txt
│ ├── .env.example
│ ├── tests/
│ │ ├── test_validation.py
│ │ └── test_schema_ctx.py
│ └── README.md
│
├── pgwatch_copilot/
│ └── pgwatch_copilot.py
│
└── README.md
```
 <img width="889" height="211" alt="image" src="https://github.com/user-attachments/assets/61f78b8c-d73d-47cf-8896-7ee945720138" /> all the tests have been passed
