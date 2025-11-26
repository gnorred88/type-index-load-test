import random
import numpy as np
from datetime import datetime, timedelta

class Generator:
    def __init__(self, heavy_prefixes_count=20):
        self.top_level = ['labs', 'pharmacy', 'billing', 'auth', 'etl', 'notifications', 'analytics']
        self.l2_labs = ['result_webhooks', 'orders', 'catalog_sync', 'providers']
        self.l3_vendors = ['quest', 'labcorp', 'bioreference', 'avalon']
        
        # Pre-generate some heavy paths to skew traffic towards
        self.heavy_paths = self._generate_heavy_paths(heavy_prefixes_count)
        
    def _generate_heavy_paths(self, count):
        paths = []
        # Ensure we have some specific ones mentioned in spec
        paths.append("labs.result_webhooks.quest")
        paths.append("labs.result_webhooks.labcorp")
        
        while len(paths) < count:
            paths.append(self._random_path(max_depth=3))
            
        return paths

    def _random_path(self, max_depth=5):
        # Zipfian-like depth choice (simplified)
        depth = np.random.zipf(2.0)  # Zipf param > 1
        if depth > max_depth:
            depth = max_depth
            
        parts = []
        # Level 1
        t1 = random.choice(self.top_level)
        parts.append(t1)
        
        if depth > 1:
            if t1 == 'labs':
                t2 = random.choice(self.l2_labs)
            else:
                t2 = f"sub_{random.randint(1, 10)}"
            parts.append(t2)
            
        if depth > 2:
            if len(parts) == 2 and parts[0] == 'labs' and parts[1] == 'result_webhooks':
                t3 = random.choice(self.l3_vendors)
            else:
                t3 = f"comp_{random.randint(1, 50)}"
            parts.append(t3)
            
        while len(parts) < depth:
            parts.append(f"node_{random.randint(1, 100)}")
            
        return ".".join(parts)

    def generate_batch_ops(self, batch_size, error_rate=0.05):
        ops = []
        # Time distribution: uniform over last 30 days, bursts in last 48h
        now = datetime.utcnow()
        
        for _ in range(batch_size):
            # 60% chance of heavy path
            if random.random() < 0.6:
                type_path = random.choice(self.heavy_paths)
            else:
                type_path = self._random_path()
                
            # Time generation
            if random.random() < 0.2: # 20% recent burst
                delta = timedelta(hours=random.uniform(0, 48))
            else:
                delta = timedelta(days=random.uniform(0, 30))
            created_at = now - delta
            
            status = 1 if random.random() < error_rate else 0
            payload = "{}" # Placeholder JSON
            
            ops.append({
                "type_path": type_path,
                "created_at": created_at,
                "status": status,
                "payload_json": payload
            })
        return ops

    @staticmethod
    def expand_prefixes(type_path):
        parts = type_path.split('.')
        prefixes = []
        for i in range(1, len(parts) + 1):
            prefixes.append(".".join(parts[:i]))
        return prefixes

