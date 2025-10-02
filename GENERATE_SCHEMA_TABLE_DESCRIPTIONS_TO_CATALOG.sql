CREATE OR REPLACE PROCEDURE GENERATE_SCHEMA_TABLE_DESCRIPTIONS_TO_CATALOG(
  database_name STRING,
  schema_name   STRING,
  use_table_data BOOLEAN,          -- TRUE lets Cortex use sample data
  catalog_table STRING,            -- e.g., 'CATALOG_TABLE' 
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

def main(session, database_name, schema_name, use_table_data, catalog_table, logging_database, logging_schema):
    db  = database_name
    sch = schema_name
    cat = catalog_table
    log_db = logging_database
    log_sch = logging_schema
    
    # Use explicit context for the target schema (where we're reading tables from)
    session.sql(f"USE DATABASE {qident(db)}").collect()
    session.sql(f"USE SCHEMA {qident(sch)}").collect()
    
    # Build fully qualified names for logging tables in centralized location
    catalog_fqn = f"{qident(log_db)}.{qident(log_sch)}.{qident(cat)}"
    errors_fqn = f"{qident(log_db)}.{qident(log_sch)}.CATALOG_ERRORS"
    
    # Ensure catalog & errors tables exist in centralized logging location
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog_fqn} (
          domain         VARCHAR,
          description    VARCHAR,
          name           VARCHAR,
          database_name  VARCHAR,
          schema_name    VARCHAR,
          table_name     VARCHAR,
          created_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """).collect()
    
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {errors_fqn} (
          database_name  VARCHAR,
          schema_name    VARCHAR,
          table_name     VARCHAR,
          error_message  VARCHAR,
          created_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """).collect()
    
    # LIST ONLY TABLES IN THE TARGET SCHEMA
    tables = session.sql("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
          AND TABLE_SCHEMA = CURRENT_SCHEMA()
    """).collect()
    
    use_flag = 'TRUE' if use_table_data else 'FALSE'
    ok_count, err_count = 0, 0
    max_errors = 200  # safety valve; adjust as you like
    
    # Names to skip (avoid describing our own catalog/error tables and other logging tables)
    skip_names = { 'CATALOG_TABLE', 'CATALOG_ERRORS', 'APPLY_DESCRIPTION_ERRORS', 'ORIGINAL_TABLE_COMMENTS_BACKUP' }
    
    for row in tables:
        tbl = row['TABLE_NAME']
        if tbl.upper() in skip_names:
            continue
            
        # FQN string passed as a single parameter
        fqn_param = f"{qident(db)}.{qident(sch)}.{qident(tbl)}"
        call_sql = (
            "CALL AI_GENERATE_TABLE_DESC(?, "
            f"OBJECT_CONSTRUCT('describe_columns', FALSE, 'use_table_data', {use_flag}))"
        )
        
        try:
            res = session.sql(call_sql, params=[fqn_param]).collect()
            import json
            out = json.loads(res[0][0])     # 1x1 JSON string
            table_desc = out['TABLE'][0]['description']
            
            # Use MERGE to overwrite existing entries (upsert behavior)
            session.sql(f"""
                MERGE INTO {catalog_fqn} AS target
                USING (SELECT 'TABLE' as domain, ? as description, ? as name, ? as database_name, ? as schema_name, ? as table_name) AS source
                ON target.domain = source.domain 
                   AND target.database_name = source.database_name 
                   AND target.schema_name = source.schema_name 
                   AND target.table_name = source.table_name
                WHEN MATCHED THEN
                    UPDATE SET 
                        description = source.description,
                        name = source.name,
                        created_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN
                    INSERT (domain, description, name, database_name, schema_name, table_name, created_at)
                    VALUES (source.domain, source.description, source.name, source.database_name, source.schema_name, source.table_name, CURRENT_TIMESTAMP())
            """, params=[table_desc, tbl, db, sch, tbl]).collect()
            
            ok_count += 1
            
        except Exception as e:
            # Insert error into centralized errors table
            session.sql(f"""
                INSERT INTO {errors_fqn}
                (database_name, schema_name, table_name, error_message)
                VALUES (?, ?, ?, ?)
            """, params=[db, sch, tbl, str(e)[:8000]]).collect()
            
            err_count += 1
            if err_count >= max_errors:
                return f"Stopped early after {err_count} errors. Success={ok_count}. Check {errors_fqn}."
    
    return f"Completed {db}.{sch}. Success={ok_count}, Errors={err_count}. Logs in {log_db}.{log_sch}. (Descriptions updated/inserted)"
$$;

CALL GENERATE_SCHEMA_TABLE_DESCRIPTIONS_TO_CATALOG('PRD_MART', 'STAR', TRUE, 'CATALOG_TABLE')

CALL GENERATE_SCHEMA_TABLE_DESCRIPTIONS_TO_CATALOG('SANDBOX', 'MHOLM', TRUE, 'CATALOG_TABLE')

-- Example usage with default centralized logging (SANDBOX.MHOLM):
-- CALL GENERATE_SCHEMA_TABLE_DESCRIPTIONS_TO_CATALOG(
--     'PRD_MART', 
--     'STAR', 
--     TRUE, 
--     'CATALOG_TABLE'
-- );

-- Example usage with custom logging location:
-- CALL GENERATE_SCHEMA_TABLE_DESCRIPTIONS_TO_CATALOG(
--     'PRD_MART', 
--     'STAR', 
--     TRUE, 
--     'CATALOG_TABLE',
--     'MY_LOG_DB',
--     'MY_LOG_SCHEMA'
-- );

-- All logging tables will now be created in SANDBOX.MHOLM (or specified location):
-- - SANDBOX.MHOLM.CATALOG_TABLE
-- - SANDBOX.MHOLM.CATALOG_ERRORS

-- Monitor progress:
-- SELECT COUNT(*) FROM SANDBOX.MHOLM.CATALOG_TABLE;
-- SELECT COUNT(*) FROM SANDBOX.MHOLM.CATALOG_ERRORS;
-- SELECT database_name, schema_name, COUNT(*) as table_count 
-- FROM SANDBOX.MHOLM.CATALOG_TABLE 
-- GROUP BY database_name, schema_name;
