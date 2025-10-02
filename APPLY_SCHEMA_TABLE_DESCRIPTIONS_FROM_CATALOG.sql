CREATE OR REPLACE PROCEDURE APPLY_SCHEMA_TABLE_DESCRIPTIONS_FROM_CATALOG(
  database_name STRING,
  schema_name   STRING,
  catalog_table STRING,           -- e.g., 'CATALOG_TABLE'
  overwrite_existing BOOLEAN,     -- TRUE = overwrite existing comments, FALSE = skip tables with comments
  backup_existing BOOLEAN,        -- TRUE = backup existing comments before overwriting
  logging_database STRING DEFAULT 'SANDBOX',  -- Where to store all logging tables
  logging_schema STRING DEFAULT 'MHOLM'       -- Schema for all logging tables
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
def qident(name: str) -> str:
    if name is None: return None
    s = str(name)
    return s if (s.isupper() and s.replace('_','').isalnum()) else '"' + s.replace('"','""') + '"'

def main(session, database_name, schema_name, catalog_table, overwrite_existing, backup_existing, logging_database, logging_schema):
    db  = database_name
    sch = schema_name
    cat = catalog_table
    log_db = logging_database
    log_sch = logging_schema
    
    # Use explicit context for target schema
    session.sql(f"USE DATABASE {qident(db)}").collect()
    session.sql(f"USE SCHEMA {qident(sch)}").collect()
    
    # Build fully qualified names for logging tables in centralized location
    catalog_fqn = f"{qident(log_db)}.{qident(log_sch)}.{qident(cat)}"
    errors_fqn = f"{qident(log_db)}.{qident(log_sch)}.APPLY_DESCRIPTION_ERRORS"
    backup_fqn = f"{qident(log_db)}.{qident(log_sch)}.ORIGINAL_TABLE_COMMENTS_BACKUP"
    
    # Create or ensure errors table exists in centralized location
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {errors_fqn} (
          database_name  VARCHAR,
          schema_name    VARCHAR,
          table_name     VARCHAR,
          error_message  VARCHAR,
          error_type     VARCHAR,
          created_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """).collect()
    
    # Create backup table for existing comments if requested in centralized location
    if backup_existing:
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {backup_fqn} (
              database_name  VARCHAR,
              schema_name    VARCHAR,
              table_name     VARCHAR,
              original_comment VARCHAR,
              backed_up_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """).collect()
    
    # Get all table descriptions from the centralized catalog
    descriptions_query = f"""
        SELECT 
            database_name,
            schema_name,
            table_name,
            description
        FROM {catalog_fqn}
        WHERE domain = 'TABLE'
          AND database_name = ?
          AND schema_name = ?
          AND description IS NOT NULL
          AND TRIM(description) != ''
    """
    
    try:
        descriptions = session.sql(descriptions_query, params=[db, sch]).collect()
    except Exception as e:
        return f"Error reading from catalog table {catalog_fqn}: {str(e)}"
    
    if not descriptions:
        return f"No table descriptions found in {catalog_fqn} for {db}.{sch}"
    
    success_count = 0
    error_count = 0
    skipped_count = 0
    backed_up_count = 0
    max_errors = 100  # Safety valve
    
    for row in descriptions:
        tbl_name = row['TABLE_NAME']
        tbl_desc = row['DESCRIPTION']
        
        # Skip our own catalog and error tables
        skip_names = {'CATALOG_TABLE', 'CATALOG_ERRORS', 'APPLY_DESCRIPTION_ERRORS', 'ORIGINAL_TABLE_COMMENTS_BACKUP'}
        if tbl_name.upper() in skip_names:
            continue
        
        # Check if table already has a comment (if we care about existing comments)
        existing_comment = None
        if not overwrite_existing or backup_existing:
            try:
                result = session.sql(f"""
                    SELECT COMMENT
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_CATALOG = ? AND TABLE_SCHEMA = ? AND TABLE_NAME = ?
                """, params=[db, sch, tbl_name]).collect()
                
                if result and len(result) > 0:
                    existing_comment = result[0]['COMMENT']
            except Exception:
                # If we can't check existing comment, proceed with caution
                existing_comment = None
        
        # Skip if table has existing comment and we don't want to overwrite
        if not overwrite_existing and existing_comment and existing_comment.strip():
            skipped_count += 1
            continue
        
        # Backup existing comment if requested to centralized location
        if backup_existing and existing_comment and existing_comment.strip():
            try:
                session.sql(f"""
                    INSERT INTO {backup_fqn} (database_name, schema_name, table_name, original_comment)
                    VALUES (?, ?, ?, ?)
                """, params=[db, sch, tbl_name, existing_comment]).collect()
                backed_up_count += 1
            except Exception as e:
                # Log backup error to centralized errors table
                session.sql(f"""
                    INSERT INTO {errors_fqn}
                    (database_name, schema_name, table_name, error_message, error_type)
                    VALUES (?, ?, ?, ?, 'BACKUP_ERROR')
                """, params=[db, sch, tbl_name, str(e)[:8000]]).collect()
            
        # Build the ALTER TABLE statement
        alter_sql = f"ALTER TABLE {qident(db)}.{qident(sch)}.{qident(tbl_name)} SET COMMENT = ?"
        
        try:
            # Apply the description as a table comment
            session.sql(alter_sql, params=[tbl_desc]).collect()
            success_count += 1
            
        except Exception as e:
            # Log the error to centralized errors table
            error_msg = str(e)[:8000]  # Truncate if too long
            session.sql(f"""
                INSERT INTO {errors_fqn}
                (database_name, schema_name, table_name, error_message, error_type)
                VALUES (?, ?, ?, ?, 'ALTER_TABLE')
            """, params=[db, sch, tbl_name, error_msg]).collect()
            
            error_count += 1
            if error_count >= max_errors:
                return f"Stopped early after {error_count} errors. Success={success_count}, Skipped={skipped_count}, Backed up={backed_up_count}. Check {errors_fqn}."
    
    return f"Completed {db}.{sch}. Success={success_count}, Errors={error_count}, Skipped={skipped_count}, Backed up={backed_up_count}. Logs in {log_db}.{log_sch}"
$$;

CALL APPLY_SCHEMA_TABLE_DESCRIPTIONS_FROM_CATALOG('PRD_MART', 'STAR', 'CATALOG_TABLE', FALSE, FALSE);

CALL APPLY_SCHEMA_TABLE_DESCRIPTIONS_FROM_CATALOG('SANDBOX', 'MHOLM', 'CATALOG_TABLE', FALSE, FALSE);


-- Example usage with default centralized logging (SANDBOX.MHOLM):
-- CALL APPLY_SCHEMA_TABLE_DESCRIPTIONS_FROM_CATALOG(
--     'PRD_MART',
--     'STAR',
--     'CATALOG_TABLE',
--     FALSE,   -- overwrite_existing
--     TRUE     -- backup_existing
-- );

-- Example usage with custom logging location:
-- CALL APPLY_SCHEMA_TABLE_DESCRIPTIONS_FROM_CATALOG(
--     'PRD_MART',
--     'STAR', 
--     'CATALOG_TABLE',
--     TRUE,    -- overwrite_existing
--     TRUE,    -- backup_existing
--     'MY_LOG_DB',
--     'MY_LOG_SCHEMA'
-- );

-- All logging tables will now be created in SANDBOX.MHOLM (or specified location):
-- - SANDBOX.MHOLM.CATALOG_TABLE (from generate procedure)
-- - SANDBOX.MHOLM.CATALOG_ERRORS (from generate procedure)  
-- - SANDBOX.MHOLM.APPLY_DESCRIPTION_ERRORS (from apply procedure)
-- - SANDBOX.MHOLM.ORIGINAL_TABLE_COMMENTS_BACKUP (from apply procedure if backup_existing = TRUE)

-- Monitoring queries for centralized logging:
-- SELECT COUNT(*) FROM SANDBOX.MHOLM.APPLY_DESCRIPTION_ERRORS;
-- SELECT COUNT(*) FROM SANDBOX.MHOLM.ORIGINAL_TABLE_COMMENTS_BACKUP;

-- Check which tables currently have comments:
-- SELECT 
--     TABLE_NAME,
--     COMMENT as existing_comment
-- FROM INFORMATION_SCHEMA.TABLES 
-- WHERE TABLE_SCHEMA = 'STAR' 
--   AND TABLE_CATALOG = 'PRD_MART'
--   AND TABLE_TYPE = 'BASE TABLE'
--   AND COMMENT IS NOT NULL 
--   AND TRIM(COMMENT) != '';

-- View all centralized logs:
-- SELECT 'catalog' as log_type, database_name, schema_name, table_name, created_at FROM SANDBOX.MHOLM.CATALOG_TABLE
-- UNION ALL
-- SELECT 'error' as log_type, database_name, schema_name, table_name, created_at FROM SANDBOX.MHOLM.CATALOG_ERRORS  
-- UNION ALL
-- SELECT 'apply_error' as log_type, database_name, schema_name, table_name, created_at FROM SANDBOX.MHOLM.APPLY_DESCRIPTION_ERRORS
-- UNION ALL  
-- SELECT 'backup' as log_type, database_name, schema_name, table_name, backed_up_at FROM SANDBOX.MHOLM.ORIGINAL_TABLE_COMMENTS_BACKUP
-- ORDER BY created_at DESC;
