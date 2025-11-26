# Ops Bench

A high-performance Python benchmark for validating the "Side Table" strategy for prefix lookups in MySQL.

This project demonstrates how to handle massive volumes of hierarchical data (e.g., `labs.result_webhooks.quest`) while maintaining sub-millisecond query latency for "Latest N items by prefix" lookups.

## Architecture

- **MySQL 8.0**:
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

## Getting Started

1. **Install Dependencies & Setup**:
   ```bash
   make install
   ```

2. **Start Database**:
   ```bash
   make up
   # Wait 10s for DB to initialize
   ```

3. **Initialize Stored Procedures**:
   ```bash
   make init-sp
   ```
   *Crucial for Mix D (Realtime workload).*

4. **Seed Data**:
   ```bash
   make seed        # 100k ops (fast)
   # OR
   make seed-full   # 10M ops (realistic)
   ```

5. **Run Benchmarks**:

   - **Mix A (Read-Heavy)**: 80% reads, focused on "Latest N".
     ```bash
     make run-a
     ```
   - **Mix B (Write-Heavy)**: 70% inserts (batched).
     ```bash
     make run-b
     ```
   - **Mix C (Mixed)**: Pagination + Counts.
     ```bash
     make run-c
     ```
   - **Mix D (Realtime)**: 50% Single-Op Inserts (Optimized SP) + 50% Read variety.
     ```bash
     make run-d
     ```

## Debugging

To view the actual data structure and verify the prefix expansion:

```bash
make debug-view
```

**Example Output:**
```text
=== Top 5 Recent Operations ===

OP #10337919
  Path:    analytics.sub_9
  Time:    2025-11-26 20:24:07.405426
  Status:  OK
  Prefixes (2):
    - analytics                                | 2025-11-26 20:24:07.405426
    - analytics.sub_9                          | 2025-11-26 20:24:07.405426
------------------------------------------------------------
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
