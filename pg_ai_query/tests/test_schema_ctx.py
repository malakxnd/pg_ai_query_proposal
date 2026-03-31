"""
tests/test_schema_ctx.py
Tests for schema introspection and context formatting in pg_ai_worker.py.

These tests mock the database connection so no live PostgreSQL is needed.

Run:
    pytest tests/test_schema_ctx.py -v
"""

import asyncio
import pytest
import sys
import os
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from middleware.pg_ai_worker import fetch_schema_context, Config


# ---------------------------------------------------------------------------
# Helpers: fake asyncpg records
# ---------------------------------------------------------------------------

def make_record(**kwargs):
    """Minimal dict-like object that mimics an asyncpg Record."""
    return kwargs


# ---------------------------------------------------------------------------
# fetch_schema_context — formats DB schema for the LLM prompt
# ---------------------------------------------------------------------------

class TestFetchSchemaContext:
    @pytest.fixture
    def cfg(self):
        return Config()

    @pytest.mark.asyncio
    async def test_basic_schema_format(self, cfg):
        from pg_ai_worker import fetch_schema_context

        # Mock pool and connection
        fake_tables = [
            make_record(table_name="users",    columns=["id integer", "email text", "created_at timestamp"]),
            make_record(table_name="orders",   columns=["id integer", "user_id integer", "total numeric"]),
            make_record(table_name="products", columns=["id integer", "name text", "price numeric"]),
        ]
        fake_version = make_record(ver=160004)  # PostgreSQL 16.4
        fake_extensions = [
            make_record(extname="plpgsql"),
            make_record(extname="pg_stat_statements"),
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=[fake_tables, fake_extensions])
        mock_conn.fetchrow = AsyncMock(return_value=fake_version)

        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_conn),
            __aexit__=AsyncMock(return_value=False),
        ))

        result = await fetch_schema_context(mock_pool, cfg)

        assert "PostgreSQL version: 16.4" in result
        assert "users" in result
        assert "orders" in result
        assert "pg_stat_statements" in result

    @pytest.mark.asyncio
    async def test_empty_schema_handled(self, cfg):
        from pg_ai_worker import fetch_schema_context

        fake_version = make_record(ver=150000)
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=[[], []])
        mock_conn.fetchrow = AsyncMock(return_value=fake_version)

        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_conn),
            __aexit__=AsyncMock(return_value=False),
        ))

        result = await fetch_schema_context(mock_pool, cfg)
        assert "no public tables found" in result
        assert "PostgreSQL version: 15.0" in result

    @pytest.mark.asyncio
    async def test_version_parsing(self, cfg):
        from pg_ai_worker import fetch_schema_context

        # PG 14.9 → ver_int = 140009
        fake_version = make_record(ver=140009)
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=[[], []])
        mock_conn.fetchrow = AsyncMock(return_value=fake_version)

        mock_pool = MagicMock()
        mock_pool.acquire = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_conn),
            __aexit__=AsyncMock(return_value=False),
        ))

        result = await fetch_schema_context(mock_pool, cfg)
        assert "14.9" in result


# ---------------------------------------------------------------------------
# Metric index (smoke test — no DB needed)
# ---------------------------------------------------------------------------

class TestMetricIndex:
    """
    Smoke tests imported directly from pgwatch_copilot.py to ensure
    the metric semantic index is consistent.
    """
    def test_imports_cleanly(self):
        # Confirm the module is importable without a live DB
        import importlib
        spec = importlib.util.spec_from_file_location(
            "pgwatch_copilot",
            os.path.join(os.path.dirname(os.path.dirname(__file__)),
                         "..", "pgwatch_copilot", "pgwatch_copilot.py")
        )
        if spec is None:
            pytest.skip("pgwatch_copilot.py not found in sibling directory")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        assert hasattr(mod, "identify_metrics")
        assert hasattr(mod, "METRIC_INDEX")

    def test_lock_maps_to_locks_table(self):
        sys.path.insert(0, os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "..", "pgwatch_copilot"
        ))
        try:
            from pgwatch_copilot import identify_metrics
            result = identify_metrics("Is there lock contention?")
            assert "locks" in result
        except ImportError:
            pytest.skip("pgwatch_copilot not available")

    def test_replication_maps_correctly(self):
        try:
            from pgwatch_copilot import identify_metrics
            result = identify_metrics("Is my replica healthy?")
            assert "replication" in result
        except ImportError:
            pytest.skip("pgwatch_copilot not available")

    def test_unknown_question_returns_defaults(self):
        try:
            from pgwatch_copilot import identify_metrics, DEFAULT_METRICS
            result = identify_metrics("What is the meaning of life?")
            assert result == DEFAULT_METRICS
        except ImportError:
            pytest.skip("pgwatch_copilot not available")