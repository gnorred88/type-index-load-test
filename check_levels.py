import time
from src.db import get_connection

def run_query(cursor, name, description, sql, params=None):
    print(f"\n{'='*80}")
    print(f"QUERY: {name}")
    print(f"DESC:  {description}")
    print(f"{'-'*80}")
    print("SQL:")
    print(sql.strip())
    if params:
        print(f"PARAMS: {params}")
    print(f"{'-'*80}")
    
    start = time.time()
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    duration = (time.time() - start) * 1000
    
    print(f"RESULT ({len(rows)} rows in {duration:.2f}ms):")
    if not rows:
        print("  (No results)")
    else:
        # Print header if dictionary cursor
        # We'll use row count to limit output
        for i, row in enumerate(rows[:5]): # Show top 5
            print(f"  Row {i+1}: {row}")
        if len(rows) > 5:
            print(f"  ... and {len(rows)-5} more")
    print(f"{'='*80}\n")

def cmd_check_levels():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        # 1. Get a sample heavy path to use for all queries
        # We'll cheat and query the DB for a deep path first
        cursor.execute("SELECT type_path FROM operations WHERE type_path LIKE '%.%.%.%' LIMIT 1")
        res = cursor.fetchone()
        if not res:
            print("Not enough data for deep paths. Using default 'labs.result_webhooks.quest'")
            path = 'labs.result_webhooks.quest'
        else:
            path = res['type_path']
            
        parts = path.split('.')
        if len(parts) < 4:
             # Fallback if the random one wasn't deep enough for L4 test
             path = 'labs.result_webhooks.quest.alpha' 
             parts = path.split('.')

        print(f"Target Path for Test: {path}")

        # Queries
        
        # Exact Match
        sql_exact = """
SELECT id, created_at, status, type_path 
FROM operations 
WHERE type_path = %s 
ORDER BY created_at DESC 
LIMIT 5
"""
        run_query(cursor, "Exact Match", "Finds rows matching the exact full path in 'operations' table.", sql_exact, (path,))

        # Levels 1-4
        for i in range(1, 5):
            if i > len(parts): 
                print(f"Skipping Level {i} (path not deep enough)")
                continue
                
            prefix = ".".join(parts[:i])
            sql_prefix = """
SELECT o.id, o.created_at, o.status, o.type_path
FROM operation_prefixes p
JOIN operations o ON o.id = p.operation_id
WHERE p.prefix = %s
ORDER BY p.created_at DESC
LIMIT 5
"""
            run_query(cursor, f"Level {i} Prefix", f"Latest 5 items under prefix '{prefix}'", sql_prefix, (prefix,))

    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    cmd_check_levels()

