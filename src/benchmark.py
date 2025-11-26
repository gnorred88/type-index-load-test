import time
import random
import threading
from src.db import get_connection
from src.generator import Generator
from src.loader import Loader

class Workload:
    def __init__(self):
        self.gen = Generator()
        self.loader = Loader()
        
    def q_latest_by_prefix(self, cursor, prefix, limit=100, offset=0):
        sql = """
            SELECT o.id, o.created_at, o.status, o.type_path
            FROM operation_prefixes p
            JOIN operations o ON o.id = p.operation_id
            WHERE p.prefix = %s
            ORDER BY p.created_at DESC
            LIMIT %s OFFSET %s
        """
        start = time.time()
        cursor.execute(sql, (prefix, limit, offset))
        cursor.fetchall()
        return time.time() - start

    def q_exact_type_path(self, cursor, path, limit=100):
        sql = """
            SELECT id, created_at, status, type_path
            FROM operations
            WHERE type_path = %s
            ORDER BY created_at DESC
            LIMIT %s
        """
        start = time.time()
        cursor.execute(sql, (path, limit))
        cursor.fetchall()
        return time.time() - start

    def q_count_24h(self, cursor, prefix):
        sql = """
            SELECT p.prefix, COUNT(*) AS cnt
            FROM operation_prefixes p
            WHERE p.created_at >= NOW() - INTERVAL 1 DAY
            AND p.prefix = %s
            GROUP BY p.prefix
        """
        start = time.time()
        cursor.execute(sql, (prefix,))
        cursor.fetchall()
        return time.time() - start

    def q_error_rate(self, cursor, prefix):
        sql = """
            SELECT p.prefix,
                   SUM(o.status=1) AS errors,
                   COUNT(*) AS total,
                   SUM(o.status=1)/COUNT(*) AS error_rate
            FROM operation_prefixes p
            JOIN operations o ON o.id = p.operation_id
            WHERE p.prefix = %s
              AND p.created_at >= NOW() - INTERVAL 7 DAY
            GROUP BY p.prefix
        """
        start = time.time()
        cursor.execute(sql, (prefix,))
        cursor.fetchall()
        return time.time() - start

    def run_mix_a(self, duration_sec):
        return self._run_loop(duration_sec, [
            (0.6, 'latest_l2'),
            (0.8, 'latest_l3'),
            (0.9, 'exact'),
            (1.0, 'count_24h')
        ])

    def run_mix_b(self, duration_sec):
        return self._run_loop(duration_sec, [
            (0.7, 'insert'),
            (0.8, 'latest_l2'),
            (0.9, 'latest_l3_cold'),
            (1.0, 'error_rate')
        ])

    def run_mix_c(self, duration_sec):
        return self._run_loop(duration_sec, [
            (0.4, 'latest_offset'),
            (0.6, 'count_24h'),
            (0.8, 'exact'),
            (1.0, 'insert_500')
        ])

    def run_mix_realtime(self, duration_sec):
        # Mix D: Realtime
        # 50% Single Optimized Inserts
        # 10% Exact
        # 10% L1
        # 10% L2
        # 10% L3
        # 10% L4
        return self._run_loop(duration_sec, [
            (0.5, 'insert_single'),
            (0.6, 'exact'),
            (0.7, 'latest_l1'),
            (0.8, 'latest_l2'),
            (0.9, 'latest_l3'),
            (1.0, 'latest_l4')
        ])

    def _run_loop(self, duration, distribution):
        start_time = time.time()
        metrics = {
            'ops': 0,
            'errors': 0,
            'latencies': [] # list of (type, latency)
        }
        
        conn = get_connection()
        cursor = conn.cursor()
        
        try:
            while time.time() - start_time < duration:
                r = random.random()
                op_type = None
                for prob, name in distribution:
                    if r < prob:
                        op_type = name
                        break
                
                try:
                    latency = 0
                    
                    if op_type == 'exact':
                        path = random.choice(self.gen.heavy_paths)
                        latency = self.q_exact_type_path(cursor, path)

                    elif op_type.startswith('latest_l'):
                        path = random.choice(self.gen.heavy_paths)
                        # For synthetic test, sometimes heavy paths aren't deep enough
                        # fallback to random path if needed
                        parts = path.split('.')
                        
                        level = int(op_type[-1]) # 1, 2, 3, 4
                        
                        if len(parts) < level:
                            # Generate deeper path if needed
                            path = self.gen._random_path(max_depth=5)
                            parts = path.split('.')
                        
                        if len(parts) >= level:
                            prefix = ".".join(parts[:level])
                            latency = self.q_latest_by_prefix(cursor, prefix, 100)
                        else:
                            # Fallback if still not deep enough (rare)
                            prefix = path
                            latency = self.q_latest_by_prefix(cursor, prefix, 100)
                            
                    elif op_type == 'latest_l3_cold':
                        path = self.gen._random_path()
                        parts = path.split('.')
                        prefix = ".".join(parts[:3]) if len(parts) >= 3 else path
                        latency = self.q_latest_by_prefix(cursor, prefix, 100)
                        
                    elif op_type == 'count_24h':
                        path = random.choice(self.gen.heavy_paths)
                        parts = path.split('.')
                        prefix = ".".join(parts[:2]) if len(parts) >= 2 else path
                        latency = self.q_count_24h(cursor, prefix)
                        
                    elif op_type == 'error_rate':
                        path = random.choice(self.gen.heavy_paths)
                        parts = path.split('.')
                        prefix = ".".join(parts[:2]) if len(parts) >= 2 else path
                        latency = self.q_error_rate(cursor, prefix)
                        
                    elif op_type == 'latest_offset':
                        path = random.choice(self.gen.heavy_paths)
                        parts = path.split('.')
                        prefix = ".".join(parts[:2]) if len(parts) >= 2 else path
                        offset = random.randint(0, 5000)
                        latency = self.q_latest_by_prefix(cursor, prefix, 100, offset)

                    elif op_type == 'insert':
                        t0 = time.time()
                        self.loader.insert_batch(1000)
                        latency = time.time() - t0
                        
                    elif op_type == 'insert_500':
                        t0 = time.time()
                        self.loader.insert_batch(500)
                        latency = time.time() - t0
                        
                    elif op_type == 'insert_single':
                        t0 = time.time()
                        self.loader.insert_single_optimized()
                        latency = time.time() - t0

                    metrics['latencies'].append((op_type, latency))
                    metrics['ops'] += 1
                    
                except Exception as e:
                    metrics['errors'] += 1
                    if metrics['errors'] <= 5:
                        print(f"Error in workload ({op_type}): {e}")
                    
        finally:
            cursor.close()
            conn.close()
            
        return metrics
