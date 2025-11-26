import time
from src.db import get_connection

def apply():
    print("Applying Stored Procedure...")
    conn = get_connection()
    cursor = conn.cursor()
    
    drop_sql = "DROP PROCEDURE IF EXISTS insert_operation_with_prefixes"
    
    create_sql = """
    CREATE PROCEDURE insert_operation_with_prefixes(
        IN p_type_path VARCHAR(191),
        IN p_created_at DATETIME(6),
        IN p_status TINYINT UNSIGNED,
        IN p_payload JSON,
        IN p_prefixes JSON
    )
    BEGIN
        DECLARE new_op_id BIGINT UNSIGNED;
        
        -- Insert operation
        INSERT INTO operations (type_path, created_at, status, payload_json)
        VALUES (p_type_path, p_created_at, p_status, p_payload);
        
        SET new_op_id = LAST_INSERT_ID();
        
        -- Insert prefixes from JSON array
        INSERT INTO operation_prefixes (operation_id, prefix, created_at)
        SELECT new_op_id, prefix, p_created_at
        FROM JSON_TABLE(
            p_prefixes,
            "$[*]" COLUMNS(prefix VARCHAR(191) PATH "$")
        ) AS jt;
    END
    """
    
    try:
        cursor.execute(drop_sql)
        cursor.execute(create_sql)
        conn.commit()
        print("Success! Stored Procedure created.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    apply()

