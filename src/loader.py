import time
import json
from src.db import get_connection
from src.generator import Generator

class Loader:
    def __init__(self):
        self.gen = Generator()

    def insert_single_optimized(self):
        conn = get_connection()
        cursor = conn.cursor()
        
        # Generate 1 op
        ops = self.gen.generate_batch_ops(1)
        op = ops[0]
        prefixes = self.gen.expand_prefixes(op['type_path'])
        prefixes_json = json.dumps(prefixes)
        
        try:
            # Use Stored Procedure
            cursor.callproc('insert_operation_with_prefixes', [
                op['type_path'],
                op['created_at'],
                op['status'],
                op['payload_json'],
                prefixes_json
            ])
            conn.commit()
            return 1
        except Exception as e:
            conn.rollback()
            print(f"Error inserting single: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def insert_batch(self, batch_size=1000):
        conn = get_connection()
        cursor = conn.cursor()
        
        ops = self.gen.generate_batch_ops(batch_size)
        
        # 1. Bulk insert operations
        # Construct INSERT INTO ... VALUES (...), (...), ...
        placeholders = "(%s, %s, %s, %s)"
        sql_ops = f"""
            INSERT INTO operations (type_path, created_at, status, payload_json)
            VALUES {', '.join([placeholders] * len(ops))}
        """
        
        # Flatten params
        val_ops = []
        for op in ops:
            val_ops.extend([op['type_path'], op['created_at'], op['status'], op['payload_json']])
            
        try:
            cursor.execute(sql_ops, val_ops)
            first_id = cursor.lastrowid
            row_count = cursor.rowcount
            
            if first_id is None or first_id == 0:
                # Should not happen with auto_increment and single INSERT
                raise Exception("Failed to retrieve lastrowid")

            # 2. Bulk insert prefixes
            val_pref_flat = []
            placeholders_pref = []
            
            for i, op in enumerate(ops):
                op_id = first_id + i
                prefixes = self.gen.expand_prefixes(op['type_path'])
                for p in prefixes:
                    placeholders_pref.append("(%s, %s, %s)")
                    val_pref_flat.extend([op_id, p, op['created_at']])
            
            if val_pref_flat:
                sql_pref = f"""
                    INSERT INTO operation_prefixes (operation_id, prefix, created_at)
                    VALUES {', '.join(placeholders_pref)}
                """
                cursor.execute(sql_pref, val_pref_flat)
                
            conn.commit()
            
            return row_count, len(placeholders_pref)
            
        except Exception as e:
            conn.rollback()
            print(f"Error inserting batch: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def run_load(self, total_ops, batch_size, workers=1):
        # This would be called by the main loop
        pass
