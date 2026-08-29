import sys 

# Task 4 helper: Run PySpark Bronze to Silver job
def run_spark_bronze_to_silver(params: dict):
    """
    Import and execute the PySpark bronze_to_silver job directly
    inside the Airflow worker process (no spark-submit needed).

    PySpark reads its SparkSession config (including the JDBC jar path)
    from within the script itself. Postgres credentials are injected via
    environment variables so the script picks them up with os.getenv().
    """

    # Add the spark directory to sys.path so we can import the module
    if params["spark_script_dir"] not in sys.path:
        sys.path.insert(0, params["spark_script_dir"])

    # Import and run — the SparkSession is created inside main()
    import bronze_to_silver
    bronze_to_silver.main()
    print("Spark job completed.")