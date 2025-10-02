CREATE OR REPLACE PROCEDURE SANDBOX.MHOLM.GENERATE_SCHEMA_TABLE_DESCRIPTIONS_TO_CATALOG("DATABASE_NAME" VARCHAR, "SCHEMA_NAME" VARCHAR, "USE_TABLE_DATA" BOOLEAN, "CATALOG_TABLE" VARCHAR)
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

def main(session, database_name, schema_name, use_table_data, catalog_table):
    db  = database_name
    sch = schema_name
    cat = catalog_table

    # Use explicit context
    session.sql(f"USE DATABASE {qident(db)}").collect()
    session.sql(f"USE SCHEMA {qident(sch)}").collect()

    # Ensure catalog & errors tables exist
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {cat} (
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
        CREATE TABLE IF NOT EXISTS {qident(db)}.{qident(sch)}.CATALOG_ERRORS (
          database_name  VARCHAR,
          schema_name    VARCHAR,
          table_name     VARCHAR,
          error_message  VARCHAR,
          created_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """).collect()

    # LIST ONLY TABLES IN THIS SCHEMA
    tables = session.sql("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = ''BASE TABLE''
          AND TABLE_SCHEMA = CURRENT_SCHEMA()
    """).collect()

    use_flag = ''TRUE'' if use_table_data else ''FALSE''
    ok_count, err_count = 0, 0
    max_errors = 200  # safety valve; adjust as you like

    # Names to skip (avoid describing our own catalog/error tables)
    skip_names = { ''CATALOG_TABLE'', ''CATALOG_ERRORS'', ''APPLY_DESCRIPTION_ERRORS'', ''ORIGINAL_TABLE_COMMENTS_BACKUP'' }

    for row in tables:
        tbl = row[''TABLE_NAME'']
        if tbl.upper() in skip_names:
            continue

        # FQN string passed as a single parameter
        fqn_param = f"{qident(db)}.{qident(sch)}.{qident(tbl)}"

        call_sql = (
            "CALL AI_GENERATE_TABLE_DESC(?, "
            f"OBJECT_CONSTRUCT(''describe_columns'', FALSE, ''use_table_data'', {use_flag}))"
        )

        try:
            res = session.sql(call_sql, params=[fqn_param]).collect()

            import json
            out = json.loads(res[0][0])     # 1x1 JSON string
            table_desc = out[''TABLE''][0][''description'']

            session.sql(f"""
                INSERT INTO {cat} (domain, description, name, database_name, schema_name, table_name)
                VALUES (''TABLE'', ?, ?, ?, ?, ?)
            """, params=[table_desc, tbl, db, sch, tbl]).collect()

            ok_count += 1

        except Exception as e:
            session.sql(f"""
                INSERT INTO {qident(db)}.{qident(sch)}.CATALOG_ERRORS
                (database_name, schema_name, table_name, error_message)
                VALUES (?, ?, ?, ?)
            """, params=[db, sch, tbl, str(e)[:8000]]).collect()

            err_count += 1
            if err_count >= max_errors:
                return f"Stopped early after {err_count} errors. Success={ok_count}. Check CATALOG_ERRORS."

    return f"Completed. Success={ok_count}, Errors={err_count}"
';
