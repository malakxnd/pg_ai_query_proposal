"""
tests/test_validation.py
Tests for the query validation pipeline in pg_ai_worker.py.

Run:
    pytest tests/test_validation.py -v
"""

import asyncio
import re
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from middleware.pg_ai_worker import extract_sql, DANGEROUS_PATTERNS

# ---------------------------------------------------------------------------
# extract_sql — strips markdown fences from LLM output
# ---------------------------------------------------------------------------

class TestExtractSql:
    def test_plain_sql_unchanged(self):
        sql = "SELECT * FROM users;"
        assert extract_sql(sql) == sql

    def test_strips_sql_fence(self):
        raw = "```sql\nSELECT * FROM users;\n```"
        assert extract_sql(raw) == "SELECT * FROM users;"

    def test_strips_plain_fence(self):
        raw = "```\nSELECT id FROM orders;\n```"
        assert extract_sql(raw) == "SELECT id FROM orders;"

    def test_strips_leading_trailing_whitespace(self):
        raw = "  \n  SELECT 1;  \n  "
        assert extract_sql(raw) == "SELECT 1;"

    def test_multiline_query_preserved(self):
        raw = "```sql\nSELECT o.id,\n       c.name\nFROM orders o\nJOIN customers c ON o.customer_id = c.id;\n```"
        result = extract_sql(raw)
        assert "SELECT o.id" in result
        assert "JOIN customers" in result

    def test_empty_string_returns_empty(self):
        assert extract_sql("") == ""

    def test_only_fences_returns_empty(self):
        assert extract_sql("```sql\n```") == ""


# ---------------------------------------------------------------------------
# DANGEROUS_PATTERNS — blocks write/DDL operations
# ---------------------------------------------------------------------------

SAFE_QUERIES = [
    "SELECT * FROM users",
    "SELECT COUNT(*) FROM orders WHERE status = 'active'",
    "SELECT u.name, SUM(o.total) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name",
    "WITH cte AS (SELECT id FROM products WHERE price > 100) SELECT * FROM cte",
    "EXPLAIN SELECT * FROM users",
]

DANGEROUS_QUERIES = [
    "DROP TABLE users",
    "DELETE FROM orders WHERE id = 1",
    "INSERT INTO users (name) VALUES ('hacker')",
    "UPDATE products SET price = 0",
    "ALTER TABLE users ADD COLUMN hack TEXT",
    "CREATE TABLE evil (id INT)",
    "TRUNCATE orders",
    "GRANT ALL ON users TO public",
    "REVOKE SELECT ON orders FROM readonly_user",
    "COPY users TO '/tmp/dump.csv'",
]


class TestDangerousPatterns:
    @pytest.mark.parametrize("sql", SAFE_QUERIES)
    def test_safe_queries_not_matched(self, sql):
        assert not DANGEROUS_PATTERNS.search(sql), \
            f"Safe query incorrectly flagged: {sql}"

    @pytest.mark.parametrize("sql", DANGEROUS_QUERIES)
    def test_dangerous_queries_matched(self, sql):
        assert DANGEROUS_PATTERNS.search(sql), \
            f"Dangerous query not caught: {sql}"

    def test_case_insensitive(self):
        assert DANGEROUS_PATTERNS.search("drop table users")
        assert DANGEROUS_PATTERNS.search("Drop Table Users")
        assert DANGEROUS_PATTERNS.search("DELETE from orders")

    def test_inline_drop_caught(self):
        # Attempt to hide DROP inside a comment-like structure
        assert DANGEROUS_PATTERNS.search("SELECT 1; DROP TABLE users;")


# ---------------------------------------------------------------------------
# Config defaults
# ---------------------------------------------------------------------------

class TestConfig:
    def test_config_defaults(self):
        from pg_ai_worker import Config
        cfg = Config()
        assert cfg.socket_path == "/tmp/pg_ai_query.sock"
        assert cfg.max_retries == 3
        assert cfg.max_schema_tables == 50
        assert cfg.llm_provider == "openai"

    def test_config_from_env(self, monkeypatch):
        from pg_ai_worker import Config
        monkeypatch.setenv("PG_AI_SOCKET", "/tmp/custom.sock")
        monkeypatch.setenv("PG_AI_MODEL", "gpt-4-turbo")
        cfg = Config.from_env()
        assert cfg.socket_path == "/tmp/custom.sock"
        assert cfg.llm_model == "gpt-4-turbo"