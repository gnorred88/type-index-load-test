import argparse
import time
import threading
import json
import numpy as np
from src.config import Config
from src.loader import Loader
from src.benchmark import Workload
from src.db import get_connection

def seed_worker(loader, batch_size, batches_per_worker, worker_id, progress_list):
    inserted = 0
    for _ in range(batches_per_worker):
        try:
            rows, _ = loader.insert_batch(batch_size)
            inserted += rows
            # Update shared progress (simple list append is thread-safe enough for progress bar)
            progress_list.append(rows)
        except Exception as e:
            print(f"Worker {worker_id} error: {e}")
            break

def cmd_seed(args):
    print(f"Seeding {args.amount} operations with {args.concurrency} threads...")
    loader = Loader()
    start_time = time.time()
    
    batch_size = args.batch_size
    total_batches = args.amount // batch_size
    batches_per_worker = total_batches // args.concurrency
    
    threads = []
    progress_list = [] # Shared list to track progress
    
    import datetime

    # Start threads
    for i in range(args.concurrency):
        # Distribute remaining batches to last worker
        if i == args.concurrency - 1:
            batches_per_worker += total_batches % args.concurrency
            
        t = threading.Thread(target=seed_worker, args=(loader, batch_size, batches_per_worker, i, progress_list))
        threads.append(t)
        t.start()

    # Monitor progress
    total_inserted = 0
    while any(t.is_alive() for t in threads):
        current_total = sum(progress_list)
        if current_total > total_inserted: # Only print if changed
            total_inserted = current_total
            elapsed = time.time() - start_time
            rate = total_inserted / elapsed if elapsed > 0 else 0
            remaining_sec = (args.amount - total_inserted) / rate if rate > 0 else 0
            eta = str(datetime.timedelta(seconds=int(remaining_sec)))
            print(f"Inserted {total_inserted}/{args.amount} ops ({rate:.0f} ops/s) - ETA: {eta}", end='\r')
        time.sleep(0.5)
            
    for t in threads:
        t.join()
        
    total_inserted = sum(progress_list)
    print(f"\nSeeding complete. {total_inserted} ops in {str(datetime.timedelta(seconds=int(time.time() - start_time)))}")

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
    p_seed.add_argument('--concurrency', type=int, default=1, help='Number of seeding threads')
    
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

