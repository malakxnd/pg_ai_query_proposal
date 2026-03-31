"""
pg_ai_query_worker.py
---------------------
Middleware daemon for pg_ai_query GSoC 2026 proposal.
Runs alongside PostgreSQL, receives natural-language queries from the C extension
via a Unix socket, and returns validated SQL.

Architecture:
  C Extension  <--(Unix socket)-->  This daemon  <--(HTTP)-->  LLM API
                                         |
                                    Context7 MCP (docs)
                                    PostgreSQL (schema)

Usage:
  python pg_ai_worker.py --socket /tmp/pg_ai_query.sock --config .env
"""

import asyncio
import json
import os
import re
import sys
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import asyncpg
import aiohttp
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("pg_ai_worker")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class Config:
    socket_path: str = "/tmp/pg_ai_query.sock"
    pg_dsn: str = "postgresql://localhost/postgres"
    llm_provider: str = "openai"          # openai | anthropic | ollama
    llm_model: str = "gpt-4o"
    llm_api_key: str = ""
    llm_api_url: str = "https://api.openai.com/v1/chat/completions"
    context7_enabled: bool = True
    context7_url: str = "https://mcp.context7.com/mcp"
    max_retries: int = 3
    max_schema_tables: int = 50           # cap to avoid blowing context window

    @classmethod
    def from_env(cls) -> "Config":
        load_dotenv()
        return cls(
            socket_path=os.getenv("PG_AI_SOCKET", "/tmp/pg_ai_query.sock"),
            pg_dsn=os.getenv("PG_AI_DSN", "postgresql://localhost/postgres"),
            llm_provider=os.getenv("PG_AI_PROVIDER", "openai"),
            llm_model=os.getenv("PG_AI_MODEL", "gpt-4o"),
            llm_api_key=os.getenv("PG_AI_API_KEY", ""),
            context7_enabled=os.getenv("PG_AI_CONTEXT7", "true").lower() == "true",
        )


# ---------------------------------------------------------------------------
# Schema introspection
# ---------------------------------------------------------------------------

SCHEMA_QUERY = """
SELECT
    t.table_name,
    array_agg(c.column_name || ' ' || c.data_type ORDER BY c.ordinal_position) AS columns
FROM information_schema.tables t
JOIN information_schema.columns c
    ON t.table_name = c.table_name AND t.table_schema = c.table_schema
WHERE t.table_schema = 'public'
  AND t.table_type = 'BASE TABLE'
GROUP BY t.table_name
ORDER BY t.table_name
LIMIT $1;
"""

VERSION_QUERY = "SELECT current_setting('server_version_num')::int AS ver;"

EXTENSIONS_QUERY = "SELECT extname FROM pg_extension ORDER BY extname;"


async def fetch_schema_context(pool: asyncpg.Pool, cfg: Config) -> str:
    """Return a compact schema description for the LLM system prompt."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(SCHEMA_QUERY, cfg.max_schema_tables)
        ver_row = await conn.fetchrow(VERSION_QUERY)
        ext_rows = await conn.fetch(EXTENSIONS_QUERY)

    ver_int = ver_row["ver"]
    major = ver_int // 10000
    minor = (ver_int % 10000) // 100 if (ver_int % 10000) >= 100 else (ver_int % 10000)

    extensions = [r["extname"] for r in ext_rows]
    tables = []
    for row in rows:
        col_list = ", ".join(row["columns"])
        tables.append(f"  {row['table_name']}({col_list})")

    schema_str = "\n".join(tables) if tables else "  (no public tables found)"
    ext_str = ", ".join(extensions) if extensions else "none"

    return (
        f"PostgreSQL version: {major}.{minor}\n"
        f"Installed extensions: {ext_str}\n"
        f"Public schema tables:\n{schema_str}"
    )


# ---------------------------------------------------------------------------
# Context7 MCP integration
# ---------------------------------------------------------------------------

async def fetch_docs_context(session: aiohttp.ClientSession, question: str, cfg: Config) -> str:
    """
    Call Context7 MCP to fetch relevant PostgreSQL documentation snippets
    for the given natural-language question.
    """
    if not cfg.context7_enabled:
        return ""

    try:
        # Context7 MCP protocol: POST /mcp with JSON-RPC 2.0
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "get-library-docs",
                "arguments": {
                    "context7CompatibleLibraryID": "/vercel/postgres",
                    "topic": question,
                    "tokens": 2000,
                },
            },
        }
        async with session.post(
            cfg.context7_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            if resp.status != 200:
                return ""
            data = await resp.json()
            content = data.get("result", {}).get("content", [])
            text_blocks = [b["text"] for b in content if b.get("type") == "text"]
            docs = "\n".join(text_blocks)[:3000]
            return f"\nRelevant PostgreSQL documentation:\n{docs}" if docs else ""
    except Exception as exc:
        log.warning("Context7 fetch failed: %s", exc)
        return ""


# ---------------------------------------------------------------------------
# Query validation
# ---------------------------------------------------------------------------

# Simple but effective: attempt to parse the SQL without executing it
DANGEROUS_PATTERNS = re.compile(
    r"\b(DROP|TRUNCATE|DELETE|INSERT|UPDATE|ALTER|CREATE|GRANT|REVOKE|COPY)\b",
    re.IGNORECASE,
)


async def validate_query(pool: asyncpg.Pool, sql: str) -> tuple[bool, str]:
    """
    Validate SQL syntax by running EXPLAIN (no execution).
    Returns (is_valid, error_message).
    """
    stripped = sql.strip().rstrip(";")

    # Block write operations — pg_ai_query is read-only
    if DANGEROUS_PATTERNS.search(stripped):
        return False, "Query contains write/DDL operations; only SELECT is allowed."

    try:
        async with pool.acquire() as conn:
            await conn.execute(f"EXPLAIN {stripped}")
        return True, ""
    except asyncpg.PostgresError as exc:
        return False, str(exc)


# ---------------------------------------------------------------------------
# LLM call
# ---------------------------------------------------------------------------

SYSTEM_PROMPT_TEMPLATE = """You are an expert PostgreSQL query generator.
Your task is to convert natural language questions into correct, read-only SQL queries.

Rules:
- Generate only SELECT statements. Never write INSERT, UPDATE, DELETE, DROP, or DDL.
- Use only tables and columns that exist in the provided schema. Do not invent names.
- Use syntax compatible with the stated PostgreSQL version.
- If you are unsure, prefer simpler queries over complex ones.
- Return ONLY the SQL query with no explanation, no markdown fences, no preamble.

{schema_context}
{docs_context}
"""


async def call_llm(
    session: aiohttp.ClientSession,
    question: str,
    schema_ctx: str,
    docs_ctx: str,
    error_feedback: Optional[str],
    cfg: Config,
) -> str:
    """Call the LLM and return the raw response text."""

    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        schema_context=schema_ctx,
        docs_context=docs_ctx,
    )

    user_message = question
    if error_feedback:
        user_message = (
            f"{question}\n\n"
            f"Your previous SQL attempt failed with this error:\n{error_feedback}\n"
            "Please generate a corrected query."
        )

    if cfg.llm_provider in ("openai", "anthropic"):
        headers = {
            "Authorization": f"Bearer {cfg.llm_api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": cfg.llm_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            "temperature": 0.0,
            "max_tokens": 1024,
        }
        async with session.post(
            cfg.llm_api_url,
            headers=headers,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data["choices"][0]["message"]["content"].strip()

    elif cfg.llm_provider == "ollama":
        # Local Ollama API
        payload = {
            "model": cfg.llm_model,
            "prompt": f"{system_prompt}\n\nUser: {user_message}\nSQL:",
            "stream": False,
        }
        async with session.post(
            "http://localhost:11434/api/generate",
            json=payload,
            timeout=aiohttp.ClientTimeout(total=60),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data["response"].strip()

    raise ValueError(f"Unknown LLM provider: {cfg.llm_provider}")


def extract_sql(raw: str) -> str:
    """Strip markdown fences and extra whitespace from LLM output."""
    # Remove ```sql ... ``` or ``` ... ```
    raw = re.sub(r"```(?:sql)?\s*", "", raw, flags=re.IGNORECASE)
    raw = raw.replace("```", "").strip()
    return raw


# ---------------------------------------------------------------------------
# Main request handler
# ---------------------------------------------------------------------------

async def handle_request(
    question: str,
    pool: asyncpg.Pool,
    session: aiohttp.ClientSession,
    cfg: Config,
) -> dict:
    """
    Full pipeline: schema → docs → LLM → validate → retry loop.
    Returns {"sql": "...", "error": None} or {"sql": None, "error": "..."}.
    """
    schema_ctx = await fetch_schema_context(pool, cfg)
    docs_ctx = await fetch_docs_context(session, question, cfg)

    last_error: Optional[str] = None

    for attempt in range(1, cfg.max_retries + 1):
        log.info("LLM attempt %d/%d for: %r", attempt, cfg.max_retries, question[:80])

        try:
            raw = await call_llm(session, question, schema_ctx, docs_ctx, last_error, cfg)
        except Exception as exc:
            return {"sql": None, "error": f"LLM call failed: {exc}"}

        sql = extract_sql(raw)
        if not sql:
            last_error = "Empty response from LLM."
            continue

        is_valid, validation_error = await validate_query(pool, sql)
        if is_valid:
            log.info("Query validated successfully on attempt %d.", attempt)
            return {"sql": sql, "error": None}

        log.warning("Validation failed (attempt %d): %s", attempt, validation_error)
        last_error = validation_error

    return {
        "sql": None,
        "error": f"Failed to generate valid SQL after {cfg.max_retries} attempts. Last error: {last_error}",
    }


# ---------------------------------------------------------------------------
# Unix socket server
# ---------------------------------------------------------------------------

async def handle_connection(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    pool: asyncpg.Pool,
    session: aiohttp.ClientSession,
    cfg: Config,
) -> None:
    """Handle a single connection from the C extension."""
    try:
        # Protocol: length-prefixed JSON
        # 4-byte big-endian uint32 → payload length → JSON bytes
        length_bytes = await reader.readexactly(4)
        length = int.from_bytes(length_bytes, "big")
        payload = await reader.readexactly(length)
        request = json.loads(payload.decode("utf-8"))

        question = request.get("question", "").strip()
        if not question:
            result = {"sql": None, "error": "Empty question."}
        else:
            result = await handle_request(question, pool, session, cfg)

        response = json.dumps(result).encode("utf-8")
        writer.write(len(response).to_bytes(4, "big") + response)
        await writer.drain()
    except Exception as exc:
        log.error("Connection handler error: %s", exc)
    finally:
        writer.close()


async def main(cfg: Config) -> None:
    socket_path = Path(cfg.socket_path)
    if socket_path.exists():
        socket_path.unlink()

    pool = await asyncpg.create_pool(cfg.pg_dsn, min_size=2, max_size=10)
    connector = aiohttp.TCPConnector(limit=20)
    session = aiohttp.ClientSession(connector=connector)

    log.info("pg_ai_worker starting on socket %s", cfg.socket_path)

    server = await asyncio.start_unix_server(
        lambda r, w: handle_connection(r, w, pool, session, cfg),
        path=cfg.socket_path,
    )

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    cfg = Config.from_env()
    try:
        asyncio.run(main(cfg))
    except KeyboardInterrupt:
        log.info("Shutting down.")
