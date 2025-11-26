import json
from src.db import get_connection

def cmd_debug_view():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        # Get top 5 most recent operations
        print("\n=== Top 5 Recent Operations ===\n")
        cursor.execute("""
            SELECT id, type_path, created_at, status, payload_json 
            FROM operations 
            ORDER BY created_at DESC 
            LIMIT 5
        """)
        ops = cursor.fetchall()
        
        for op in ops:
            print(f"OP #{op['id']}")
            print(f"  Path:    {op['type_path']}")
            print(f"  Time:    {op['created_at']}")
            print(f"  Status:  {'ERROR' if op['status'] else 'OK'}")
            
            # Get related prefixes
            cursor.execute("""
                SELECT prefix, created_at 
                FROM operation_prefixes 
                WHERE operation_id = %s 
                ORDER BY length(prefix) ASC
            """, (op['id'],))
            prefixes = cursor.fetchall()
            
            print(f"  Prefixes ({len(prefixes)}):")
            for p in prefixes:
                print(f"    - {p['prefix']:<40} | {p['created_at']}")
            print("-" * 60)
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    cmd_debug_view()

