CREATE OR REPLACE PROCEDURE SANDBOX.MHOLM.APPLY_SCHEMA_TABLE_DESCRIPTIONS_FROM_CATALOG("DATABASE_NAME" VARCHAR, "SCHEMA_NAME" VARCHAR, "CATALOG_TABLE" VARCHAR, "OVERWRITE_EXISTING" BOOLEAN, "BACKUP_EXISTING" BOOLEAN)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS '
def qident(name: str) -> str:
    if name is None: return None
    s = str(name)
    return s if (s.isupper() and s.replace(''_'','''').isalnum()) else ''"'' + s.replace(''"'',''""'') + ''"''

def main(session, database_name, schema_name, catalog_table, overwrite_existing, backup_existing):
    db  = database_name
    sch = schema_name
    cat = catalog_table
    
    # Use explicit context
    session.sql(f"USE DATABASE {qident(db)}").collect()
    session.sql(f"USE SCHEMA {qident(sch)}").collect()
    
    # Create or ensure errors table exists for tracking any issues
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {qident(db)}.{qident(sch)}.APPLY_DESCRIPTION_ERRORS (
          database_name  VARCHAR,
          schema_name    VARCHAR,
          table_name     VARCHAR,
          error_message  VARCHAR,
          error_type     VARCHAR,
          created_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """).collect()
    
    # Create backup table for existing comments if requested
    backup_table = None
    if backup_existing:
        backup_table = f"{qident(db)}.{qident(sch)}.ORIGINAL_TABLE_COMMENTS_BACKUP"
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {backup_table} (
              database_name  VARCHAR,
              schema_name    VARCHAR,
              table_name     VARCHAR,
              original_comment VARCHAR,
              backed_up_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """).collect()
    
    # Get all table descriptions from the catalog
    descriptions_query = f"""
        SELECT 
            database_name,
            schema_name,
            table_name,
            description
        FROM {cat}
        WHERE domain = ''TABLE''
          AND database_name = ?
          AND schema_name = ?
          AND description IS NOT NULL
          AND TRIM(description) != ''''
    """
    
    try:
        descriptions = session.sql(descriptions_query, params=[db, sch]).collect()
    except Exception as e:
        return f"Error reading from catalog table {cat}: {str(e)}"
    
    if not descriptions:
        return f"No table descriptions found in {cat} for {db}.{sch}"
    
    success_count = 0
    error_count = 0
    skipped_count = 0
    backed_up_count = 0
    max_errors = 100  # Safety valve
    
    for row in descriptions:
        tbl_name = row[''TABLE_NAME'']
        tbl_desc = row[''DESCRIPTION'']
        
        # Skip our own catalog and error tables
        skip_names = {''CATALOG_TABLE'', ''CATALOG_ERRORS'', ''APPLY_DESCRIPTION_ERRORS'', ''ORIGINAL_TABLE_COMMENTS_BACKUP''}
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
                    existing_comment = result[0][''COMMENT'']
            except Exception:
                # If we can''t check existing comment, proceed with caution
                existing_comment = None
        
        # Skip if table has existing comment and we don''t want to overwrite
        if not overwrite_existing and existing_comment and existing_comment.strip():
            skipped_count += 1
            continue
        
        # Backup existing comment if requested
        if backup_existing and existing_comment and existing_comment.strip() and backup_table:
            try:
                session.sql(f"""
                    INSERT INTO {backup_table} (database_name, schema_name, table_name, original_comment)
                    VALUES (?, ?, ?, ?)
                """, params=[db, sch, tbl_name, existing_comment]).collect()
                backed_up_count += 1
            except Exception as e:
                # Log backup error but continue with applying new description
                session.sql(f"""
                    INSERT INTO {qident(db)}.{qident(sch)}.APPLY_DESCRIPTION_ERRORS
                    (database_name, schema_name, table_name, error_message, error_type)
                    VALUES (?, ?, ?, ?, ''BACKUP_ERROR'')
                """, params=[db, sch, tbl_name, str(e)[:8000]]).collect()
            
        # Build the ALTER TABLE statement
        alter_sql = f"ALTER TABLE {qident(db)}.{qident(sch)}.{qident(tbl_name)} SET COMMENT = ?"
        
        try:
            # Apply the description as a table comment
            session.sql(alter_sql, params=[tbl_desc]).collect()
            success_count += 1
            
        except Exception as e:
            # Log the error
            error_msg = str(e)[:8000]  # Truncate if too long
            session.sql(f"""
                INSERT INTO {qident(db)}.{qident(sch)}.APPLY_DESCRIPTION_ERRORS
                (database_name, schema_name, table_name, error_message, error_type)
                VALUES (?, ?, ?, ?, ''ALTER_TABLE'')
            """, params=[db, sch, tbl_name, error_msg]).collect()
            
            error_count += 1
            if error_count >= max_errors:
                return f"Stopped early after {error_count} errors. Success={success_count}, Skipped={skipped_count}, Backed up={backed_up_count}. Check APPLY_DESCRIPTION_ERRORS table."
    
    return f"Completed applying descriptions. Success={success_count}, Errors={error_count}, Skipped={skipped_count}, Backed up={backed_up_count}"
';
