import os

class Config:
    DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
    DB_PORT = int(os.getenv("DB_PORT", "3306"))
    DB_USER = os.getenv("DB_USER", "root")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "root")
    DB_NAME = os.getenv("DB_NAME", "ops_bench")
    
    # Workload config
    TOTAL_OPS = int(os.getenv("TOTAL_OPS", "10000000"))
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
    CONCURRENCY = int(os.getenv("CONCURRENCY", "10"))
    
    # Generator config
    DEPTH_MEAN = int(os.getenv("DEPTH_MEAN", "3"))
    HEAVY_PREFIXES_COUNT = int(os.getenv("HEAVY_PREFIXES_COUNT", "20"))
    ERROR_RATE = float(os.getenv("ERROR_RATE", "0.05"))

