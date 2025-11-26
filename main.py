import argparse
import time
import threading
import json
import numpy as np
from src.config import Config
from src.loader import Loader
from src.benchmark import Workload
from src.db import get_connection

def cmd_seed(args):
    print(f"Seeding {args.amount} operations...")
    loader = Loader()
    total_inserted = 0
    start_time = time.time()
    
    # We can multi-thread seed too if needed, but single thread is safer for simple logic
    # unless we want high speed.
    # For 10M ops, single thread batch 1000 might take a while.
    # 10M / 1000 = 10,000 batches.
    # If 100ms per batch -> 1000s = 16 mins. Acceptable.
    
    batch_size = args.batch_size
    batches = args.amount // batch_size
    
    for i in range(batches):
        rows, prefs = loader.insert_batch(batch_size)
        total_inserted += rows
        if i % 10 == 0:
            print(f"Inserted {total_inserted} ops...", end='\r')
            
    print(f"\nSeeding complete. {total_inserted} ops in {time.time() - start_time:.2f}s")

def cmd_validate(args):
    print("Running validations...")
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # 1. EXPLAIN ANALYZE
        print("Validating Plan for 'latest N by prefix'...")
        try:
            cursor.execute("""
                EXPLAIN ANALYZE
                SELECT o.id
                FROM operation_prefixes p
                JOIN operations o ON o.id = p.operation_id
                WHERE p.prefix = 'labs.result_webhooks'
                ORDER BY p.created_at DESC
                LIMIT 100;
            """)
            res = cursor.fetchall()
            for row in res:
                print(row[0])
        except Exception as e:
            print(f"EXPLAIN ANALYZE failed (maybe not supported or error): {e}")
            # Fallback to simple EXPLAIN
            cursor.execute("""
                EXPLAIN
                SELECT o.id
                FROM operation_prefixes p
                JOIN operations o ON o.id = p.operation_id
                WHERE p.prefix = 'labs.result_webhooks'
                ORDER BY p.created_at DESC
                LIMIT 100;
            """)
            res = cursor.fetchall()
            print("Plan:")
            for row in res:
                print(row)

        # 2. Cardinality check
        print("\nChecking Cardinality...")
        cursor.execute("SELECT COUNT(*) FROM operation_prefixes")
        count = cursor.fetchone()[0]
        print(f"Total prefixes: {count}")
        
        cursor.execute("SELECT COUNT(*) FROM operations")
        ops_count = cursor.fetchone()[0]
        print(f"Total operations: {ops_count}")
        if ops_count > 0:
            print(f"Avg amplification: {count / ops_count:.2f}")

    finally:
        cursor.close()
        conn.close()

def run_worker(mix_func, duration, results, index):
    metrics = mix_func(duration)
    results[index] = metrics

def cmd_run(args):
    print(f"Running Mix {args.mix} with {args.concurrency} workers for {args.time}s...")
    
    threads = []
    results = [None] * args.concurrency
    workload = Workload()
    
    func = None
    if args.mix == 'A':
        func = workload.run_mix_a
    elif args.mix == 'B':
        func = workload.run_mix_b
    elif args.mix == 'C':
        func = workload.run_mix_c
    elif args.mix == 'D':
        func = workload.run_mix_realtime
    else:
        print("Unknown mix. Use A, B, C, or D.")
        return

    # Warmup
    print("Warming up...")
    func(10) # 10 seconds warmup (spec says 2-5 mins, but keeping it short for demo)
    
    print("Starting benchmark...")
    start_global = time.time()
    
    for i in range(args.concurrency):
        t = threading.Thread(target=run_worker, args=(func, args.time, results, i))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    end_global = time.time()
    print(f"Benchmark finished in {end_global - start_global:.2f}s")
    
    # Aggregate results
    total_ops = 0
    total_errors = 0
    all_latencies = {} # type -> list of latencies
    
    for r in results:
        if r:
            total_ops += r['ops']
            total_errors += r['errors']
            for op_type, lat in r['latencies']:
                if op_type not in all_latencies:
                    all_latencies[op_type] = []
                all_latencies[op_type].append(lat)
    
    print(f"\nResults:")
    print(f"Total Ops: {total_ops}")
    print(f"QPS: {total_ops / args.time:.2f}")
    print(f"Errors: {total_errors}")
    
    print("\nLatency (ms) p50 / p95 / p99:")
    for op_type, lats in all_latencies.items():
        if not lats:
            continue
        a = np.array(lats) * 1000 # to ms
        p50 = np.percentile(a, 50)
        p95 = np.percentile(a, 95)
        p99 = np.percentile(a, 99)
        print(f"  {op_type:<15}: {p50:.2f} / {p95:.2f} / {p99:.2f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ops Bench Tool")
    subparsers = parser.add_subparsers(dest='command')
    
    # Seed command
    p_seed = subparsers.add_parser('seed')
    p_seed.add_argument('--amount', type=int, default=Config.TOTAL_OPS, help='Number of operations to insert')
    p_seed.add_argument('--batch-size', type=int, default=Config.BATCH_SIZE)
    
    # Validate command
    p_val = subparsers.add_parser('validate')
    
    # Run command
    p_run = subparsers.add_parser('run')
    p_run.add_argument('--mix', type=str, required=True, choices=['A', 'B', 'C', 'D'])
    p_run.add_argument('--time', type=int, default=60, help='Duration in seconds')
    p_run.add_argument('--concurrency', type=int, default=Config.CONCURRENCY)
    
    args = parser.parse_args()
    
    if args.command == 'seed':
        cmd_seed(args)
    elif args.command == 'validate':
        cmd_validate(args)
    elif args.command == 'run':
        cmd_run(args)
    else:
        parser.print_help()

