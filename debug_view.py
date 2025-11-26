import json
from src.db import get_connection

def cmd_debug_view():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        # Table Statistics
        print("\n=== Table Statistics ===\n")
        
        # Get sizes from information_schema
        cursor.execute("""
            SELECT 
                table_name AS `Table`, 
                round(((data_length + index_length) / 1024 / 1024), 2) `Size in MB`,
                table_rows AS `Row Count`
            FROM information_schema.TABLES 
            WHERE table_schema = 'ops_bench'
            AND table_name IN ('operations', 'operation_prefixes');
        """)
        stats = cursor.fetchall()
        
        # Print formatted table
        print(f"{'Table':<25} | {'Rows (Approx)':<15} | {'Size (MB)':<10}")
        print("-" * 56)
        for row in stats:
            print(f"{row['Table']:<25} | {row['Row Count']:<15} | {row['Size in MB']:<10}")
        
        # Calculate Amplification Factor
        ops_count = next((r['Row Count'] for r in stats if r['Table'] == 'operations'), 0)
        pref_count = next((r['Row Count'] for r in stats if r['Table'] == 'operation_prefixes'), 0)
        
        if ops_count > 0:
            amp = pref_count / ops_count
            print("-" * 56)
            print(f"Amplification Factor: {amp:.2f}x (Prefix Rows / Operation Rows)")

        # Recent Operations
        print("\n\n=== Top 5 Recent Operations ===\n")
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
