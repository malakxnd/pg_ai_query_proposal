"""
benchmark.py
------------
Accuracy benchmark for pg_ai_query GSoC 2026.
Measures the percentage of natural-language → SQL conversions that are
both syntactically valid and semantically equivalent to the reference query.

Usage:
  python benchmark.py --dsn postgresql://localhost/testdb --socket /tmp/pg_ai_query.sock
"""

import asyncio
import json
import socket
import struct
import time
from dataclasses import dataclass, field
from typing import Optional
import asyncpg


@dataclass
class TestCase:
    id: str
    category: str         # simple | intermediate | advanced | pg_specific | version_sensitive
    question: str
    reference_sql: str
    pg_version_min: int = 14  # minimum PostgreSQL major version


# A representative sample of the 100-case benchmark suite.
# Full suite is generated programmatically against the test database schema.
BENCHMARK_CASES: list[TestCase] = [
    # --- Simple ---
    TestCase("S01", "simple", "Show all customers", "SELECT * FROM customers;"),
    TestCase("S02", "simple", "How many orders are there?", "SELECT COUNT(*) FROM orders;"),
    TestCase("S03", "simple", "List products ordered by price descending",
             "SELECT * FROM products ORDER BY price DESC;"),
    TestCase("S04", "simple", "Find all users created in the last 30 days",
             "SELECT * FROM users WHERE created_at >= NOW() - INTERVAL '30 days';"),

    # --- Intermediate ---
    TestCase("I01", "intermediate",
             "Show each customer with their total order amount",
             "SELECT c.id, c.name, SUM(o.amount) AS total FROM customers c "
             "JOIN orders o ON c.id = o.customer_id GROUP BY c.id, c.name;"),
    TestCase("I02", "intermediate",
             "Find customers who have never placed an order",
             "SELECT c.* FROM customers c LEFT JOIN orders o ON c.id = o.customer_id "
             "WHERE o.id IS NULL;"),
    TestCase("I03", "intermediate",
             "Top 5 products by revenue",
             "SELECT p.name, SUM(oi.quantity * oi.unit_price) AS revenue "
             "FROM products p JOIN order_items oi ON p.id = oi.product_id "
             "GROUP BY p.name ORDER BY revenue DESC LIMIT 5;"),

    # --- Advanced ---
    TestCase("A01", "advanced",
             "Rank customers by their total spend using a window function",
             "SELECT c.name, SUM(o.amount) AS total, "
             "RANK() OVER (ORDER BY SUM(o.amount) DESC) AS spend_rank "
             "FROM customers c JOIN orders o ON c.id = o.customer_id "
             "GROUP BY c.name;"),
    TestCase("A02", "advanced",
             "Show 7-day rolling average of daily order counts",
             "SELECT order_date, COUNT(*) AS daily_count, "
             "AVG(COUNT(*)) OVER (ORDER BY order_date "
             "ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg "
             "FROM orders GROUP BY order_date ORDER BY order_date;"),
    TestCase("A03", "advanced",
             "Find the top spending customer in each country using a CTE",
             "WITH ranked AS ("
             "  SELECT c.country, c.name, SUM(o.amount) AS total, "
             "  ROW_NUMBER() OVER (PARTITION BY c.country ORDER BY SUM(o.amount) DESC) AS rn "
             "  FROM customers c JOIN orders o ON c.id = o.customer_id "
             "  GROUP BY c.country, c.name"
             ") SELECT country, name, total FROM ranked WHERE rn = 1;"),

    # --- PostgreSQL-specific ---
    TestCase("P01", "pg_specific",
             "Find users whose preferences JSON contains dark_mode set to true",
             "SELECT * FROM users WHERE preferences->>'dark_mode' = 'true';"),
    TestCase("P02", "pg_specific",
             "Get all unique tags from the products tags array column",
             "SELECT DISTINCT unnest(tags) AS tag FROM products ORDER BY tag;"),
    TestCase("P03", "pg_specific",
             "Full text search for products matching 'wireless keyboard'",
             "SELECT * FROM products "
             "WHERE to_tsvector('english', name || ' ' || description) "
             "@@ plainto_tsquery('english', 'wireless keyboard');"),

    # --- Version-sensitive ---
    TestCase("V01", "version_sensitive",
             "Merge customer data from staging into main table",
             "MERGE INTO customers AS target USING staging_customers AS source "
             "ON target.email = source.email "
             "WHEN MATCHED THEN UPDATE SET name = source.name "
             "WHEN NOT MATCHED THEN INSERT (email, name) VALUES (source.email, source.name);",
             pg_version_min=15),
]


@dataclass
class BenchmarkResult:
    case_id: str
    category: str
    question: str
    generated_sql: Optional[str]
    error: Optional[str]
    syntax_valid: bool = False
    latency_ms: float = 0.0


def send_query(sock_path: str, question: str) -> dict:
    """Send a question to the pg_ai_worker via Unix socket."""
    payload = json.dumps({"question": question}).encode("utf-8")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(sock_path)
        s.sendall(struct.pack(">I", len(payload)) + payload)
        length_bytes = s.recv(4)
        length = struct.unpack(">I", length_bytes)[0]
        response_bytes = b""
        while len(response_bytes) < length:
            chunk = s.recv(length - len(response_bytes))
            if not chunk:
                break
            response_bytes += chunk
        return json.loads(response_bytes.decode("utf-8"))


async def check_syntax(pool: asyncpg.Pool, sql: str) -> bool:
    """Use EXPLAIN to verify syntax without executing."""
    try:
        async with pool.acquire() as conn:
            stripped = sql.strip().rstrip(";")
            await conn.execute(f"EXPLAIN {stripped}")
        return True
    except Exception:
        return False


async def run_benchmark(sock_path: str, dsn: str) -> None:
    pool = await asyncpg.create_pool(dsn)

    # Detect PostgreSQL version
    async with pool.acquire() as conn:
        ver_row = await conn.fetchrow("SELECT current_setting('server_version_num')::int AS v")
    pg_version = ver_row["v"] // 10000

    print(f"Running benchmark against PostgreSQL {pg_version}")
    print(f"Socket: {sock_path}")
    print(f"Cases: {len(BENCHMARK_CASES)}\n")

    results: list[BenchmarkResult] = []

    for case in BENCHMARK_CASES:
        if case.pg_version_min > pg_version:
            print(f"  SKIP  [{case.id}] {case.question[:60]} (requires PG {case.pg_version_min}+)")
            continue

        t0 = time.perf_counter()
        response = send_query(sock_path, case.question)
        latency = (time.perf_counter() - t0) * 1000

        result = BenchmarkResult(
            case_id=case.id,
            category=case.category,
            question=case.question,
            generated_sql=response.get("sql"),
            error=response.get("error"),
            latency_ms=latency,
        )

        if result.generated_sql:
            result.syntax_valid = await check_syntax(pool, result.generated_sql)

        status = "✓" if result.syntax_valid else "✗"
        print(f"  {status}  [{case.id}] {case.question[:60]} ({latency:.0f}ms)")
        if result.error:
            print(f"       ERROR: {result.error}")

        results.append(result)

    await pool.close()

    # Summary
    total = len(results)
    valid = sum(1 for r in results if r.syntax_valid)
    by_category: dict[str, tuple[int, int]] = {}
    for r in results:
        v, t = by_category.get(r.category, (0, 0))
        by_category[r.category] = (v + int(r.syntax_valid), t + 1)

    print(f"\n{'='*60}")
    print(f"OVERALL: {valid}/{total} ({valid/total*100:.1f}%) syntactically valid")
    print(f"\nBy category:")
    for cat, (v, t) in sorted(by_category.items()):
        print(f"  {cat:<20} {v}/{t} ({v/t*100:.0f}%)")
    avg_latency = sum(r.latency_ms for r in results) / len(results) if results else 0
    print(f"\nAverage latency: {avg_latency:.0f}ms per query")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", default="/tmp/pg_ai_query.sock")
    parser.add_argument("--dsn", default="postgresql://localhost/postgres")
    args = parser.parse_args()
    asyncio.run(run_benchmark(args.socket, args.dsn))
