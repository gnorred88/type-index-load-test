-- Schema for ops_bench
-- Run this to initialize the database

CREATE DATABASE IF NOT EXISTS ops_bench;

USE ops_bench;

-- Main table
CREATE TABLE IF NOT EXISTS operations (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  type_path     VARCHAR(191) NOT NULL,    -- e.g. 'labs.result_webhooks.quest'
  created_at    DATETIME(6)   NOT NULL,   -- microsecond precision for realistic bursts
  status        TINYINT UNSIGNED NOT NULL, -- 0=ok,1=error (compact)
  payload_json  JSON NULL,
  PRIMARY KEY (id),
  KEY ix_created_at (created_at),
  KEY ix_type_path (type_path),
  CHECK (type_path REGEXP '^[a-z0-9_]+(\\.[a-z0-9_]+)*$')
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

-- Side table: one row per ancestor prefix (including the full path)
-- Denormalize created_at to support super-fast "latest N by prefix"
CREATE TABLE IF NOT EXISTS operation_prefixes (
  operation_id  BIGINT UNSIGNED NOT NULL,
  prefix        VARCHAR(191) NOT NULL,     -- 'labs', 'labs.result_webhooks', 'labs.result_webhooks.quest'
  created_at    DATETIME(6) NOT NULL,      -- copied from operations.created_at
  PRIMARY KEY (operation_id, prefix),      -- uniqueness + compact clustering
  KEY ix_prefix_created (prefix, created_at DESC, operation_id) -- top-K scans
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

-- Stored Procedure for optimized single-row insertion
DROP PROCEDURE IF EXISTS insert_operation_with_prefixes;

DELIMITER //

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
END //

DELIMITER ;
