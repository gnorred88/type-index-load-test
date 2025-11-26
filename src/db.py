import mysql.connector
from mysql.connector import pooling
from src.config import Config

_pool = None

def get_pool():
    global _pool
    if _pool is None:
        _pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="mypool",
            pool_size=min(32, (Config.CONCURRENCY * 2) + 5), # Capped at 32 by mysql-connector-python
            pool_reset_session=True,
            host=Config.DB_HOST,
            port=Config.DB_PORT,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            database=Config.DB_NAME,
            autocommit=False # Important for batching
        )
    return _pool

def get_connection():
    pool = get_pool()
    return pool.get_connection()

