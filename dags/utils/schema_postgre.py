SCHEMA_POSTGRES = """
        CREATE SCHEMA IF NOT EXISTS bronze;

        DROP TABLE IF EXISTS bronze.bronze_fintech_raw;
        
        CREATE TABLE bronze.bronze_fintech_raw (
            id               SERIAL PRIMARY KEY,
            raw_data         JSONB        NOT NULL,
            load_timestamp   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
        );

        CREATE SCHEMA IF NOT EXISTS silver;

        DROP TABLE IF EXISTS silver.stg_customers;
        
        CREATE TABLE silver.stg_customers (
            customer_id             VARCHAR,
            first_name              VARCHAR,
            last_name               VARCHAR,
            email                   VARCHAR,
            phone_number            VARCHAR,
            date_of_birth           VARCHAR,
            gender                  VARCHAR,
            nationality             VARCHAR,
            city                    VARCHAR,
            country                 VARCHAR,
            address                 VARCHAR,
            lat                     VARCHAR,
            lon                     VARCHAR,
            registration_date       VARCHAR,
            kyc_status              VARCHAR,
            risk_score              VARCHAR,
            customer_segment        VARCHAR,
            relationship_manager    VARCHAR,
            status                  VARCHAR,
            credit_info             VARCHAR,
            digital_engagement      VARCHAR,
            load_timestamp          TIMESTAMP
        );

        DROP TABLE IF EXISTS silver.stg_accounts;
        
        CREATE TABLE silver.stg_accounts (
            customer_id     VARCHAR,
            account_id      VARCHAR,
            account_type    VARCHAR,
            currency        VARCHAR,
            balance         VARCHAR,
            credit_limit    VARCHAR,
            interest_rate   VARCHAR,
            opened_date     VARCHAR,
            status          VARCHAR,
            branch_code     VARCHAR,
            load_timestamp  TIMESTAMP
        );

        DROP TABLE IF EXISTS silver.stg_transactions;
        
        CREATE TABLE silver.stg_transactions (
            customer_id     VARCHAR,
            transaction_id  VARCHAR,
            account_id      VARCHAR,
            date            VARCHAR,
            amount          VARCHAR,
            currency        VARCHAR,
            type            VARCHAR,
            category        VARCHAR,
            merchant        VARCHAR,
            channel         VARCHAR,
            status          VARCHAR,
            description     VARCHAR,
            load_timestamp  TIMESTAMP
        );

        DROP TABLE IF EXISTS silver.stg_loans;
        
        CREATE TABLE silver.stg_loans (
            customer_id         VARCHAR,
            loan_id             VARCHAR,
            type                VARCHAR,
            currency            VARCHAR,
            principal           VARCHAR,
            outstanding_balance VARCHAR,
            interest_rate       VARCHAR,
            term_months         VARCHAR,
            monthly_payment     VARCHAR,
            start_date          VARCHAR,
            end_date            VARCHAR,
            status              VARCHAR,
            days_past_due       VARCHAR,
            collateral_type     VARCHAR,
            load_timestamp      TIMESTAMP
        );

    """