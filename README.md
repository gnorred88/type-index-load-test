# Ops Bench

A Python benchmark for validating the "Side Table" strategy for prefix lookups in MySQL.

This project demonstrates how to handle large volumes of hierarchical data (e.g., `labs.result_webhooks.quest`) while maintaining sub-millisecond query latency for "Latest N items by prefix" lookups.

## Architecture

- **MySQL 8.0+**:
    - `operations`: Main log of events (append-only).
    - `operation_prefixes`: Denormalized side table (one row per ancestor prefix).
    - **Optimization**: Custom Stored Procedure (`insert_operation_with_prefixes`) for atomic, low-latency insertion of both records.
- **Python**:
    - Custom load generator with Zipfian distribution.
    - Multi-threaded benchmark runner.
    - Connection pooling with `mysql-connector-python`.

## Prerequisites

- Docker & Docker Compose
- Python 3.8+
- Make

## Command Reference

### Setup & Data Management

| Command | Description |
| :--- | :--- |
| `make install` | Creates a local `.venv`, installs Python dependencies. |
| `make up` | Starts MySQL in Docker with optimized durability settings. |
| `make down` | Stops and removes the Docker containers. |
| `make init-sp` | Applies the stored procedure (`insert_operation_with_prefixes`) required for Mix D. |
| `make clean` | Removes the virtual environment and `__pycache__`. |

### Seeding Data

| Command | Description |
| :--- | :--- |
| `make seed-fast` | Inserts **1M** operations. Quick smoke test (~1 min). |
| `make seed` | Inserts **10M** operations. Standard baseline (~10 mins). |
| `make seed-full` | Inserts **100M** operations. Stress test (~2 hours). Uses 8 threads. |

### Benchmarks

| Command | Description | Workload Mix |
| :--- | :--- | :--- |
| `make run-a` | **Read-Heavy** | 80% Reads (Latest N), 20% Misc. |
| `make run-b` | **Write-Heavy** | 70% Batched Inserts, 30% Reads. |
| `make run-c` | **Mixed** | Pagination + Windowed Counts + Writes. |
| `make run-d` | **Realtime** | 50% Single-Row Inserts (SP), 50% Reads (L1-L4 depth + Exact). |

### Debugging & Inspection

| Command | Description |
| :--- | :--- |
| `make debug-records` | Shows table sizes, top 5 most recent operations, and their expanded prefix rows in the side table. |
| `make debug-sql` | Runs exact queries for Exact Match + Level 1-4 Prefixes and prints the SQL plans + execution time. |
| `make validate` | Runs `EXPLAIN ANALYZE` on the core query to verify index usage. |

## Debugging Examples

**`make debug-records` Output:**
```text
=== Table Statistics ===

Table                     | Rows (Approx)   | Size (MB) 
--------------------------------------------------------
operation_prefixes        | 34011808        | 3618.00   
operations                | 17918610        | 2349.95   
--------------------------------------------------------
Amplification Factor: 1.90x (Prefix Rows / Operation Rows)


=== Top 5 Recent Operations ===

OP #18817390
  Path:    notifications.sub_8.comp_1
  Time:    2025-11-26 21:07:56.375881
  Status:  OK
  Prefixes (3):
    - notifications                            | 2025-11-26 21:07:56.375881
    - notifications.sub_8                      | 2025-11-26 21:07:56.375881
    - notifications.sub_8.comp_1               | 2025-11-26 21:07:56.375881
------------------------------------------------------------
```

**`make debug-sql` Output:**
```text
================================================================================
QUERY: Level 3 Prefix
DESC:  Latest 5 items under prefix 'labs.result_webhooks.quest'
--------------------------------------------------------------------------------
SQL:
SELECT o.id, o.created_at, o.status, o.type_path
FROM operation_prefixes p
JOIN operations o ON o.id = p.operation_id
WHERE p.prefix = %s
ORDER BY p.created_at DESC
LIMIT 5
...
RESULT (5 rows in 0.82ms):
...
```

## Benchmark Results (Example)

Running `Mix D` (Realtime) on a standard dev environment:

```text
Running Mix D with 8 workers for 60s...
...
Results:
Total Ops: 64590
QPS: 1076.50
Errors: 0

Latency (ms) p50 / p95 / p99:
  latest_l1      : 1.72 / 7.53 / 23.77
  insert_single  : 8.99 / 20.76 / 39.46
  latest_l4      : 1.54 / 6.76 / 33.66
  latest_l3      : 1.85 / 9.27 / 43.05
  exact          : 4.17 / 32.53 / 56.04
  latest_l2      : 1.74 / 7.65 / 33.57
```

*Note how `latest_l4` (deepest prefix) performs comparably to `latest_l1` due to the direct index seek on `(prefix, created_at)`.*

## Configuration

Environment variables can be set in `.env` or passed to the shell:

- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`
- `TOTAL_OPS`: Target seed count.
- `CONCURRENCY`: Number of worker threads (Default: 8).
- `BATCH_SIZE`: Rows per insert batch.
