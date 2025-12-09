from datetime import datetime

def log_job_execution(status, message=""):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] JOB {status}: {message}")

def check_and_setup_schema():
    from config import config
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.SCHEMA_NAME}")
        spark.sql(f"USE {config.SCHEMA_NAME}")
        config.use_schema = True
    except:
        config.use_schema = False