SCHEMA_POSTGRES = """
        CREATE SCHEMA IF NOT EXISTS bronze;

        DROP TABLE IF EXISTS bronze.bronze_fintech_raw;
        
        CREATE TABLE bronze.bronze_fintech_raw (
            id               SERIAL PRIMARY KEY,
            raw_data         JSONB        NOT NULL,
            load_timestamp   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
        );

    """