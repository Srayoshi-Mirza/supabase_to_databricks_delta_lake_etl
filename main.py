from datetime import datetime
from utils import log_job_execution, check_and_setup_schema
from extract import extract_incremental, extract_full
from transform import clean_and_enrich
from analytics import create_daily_summaries, create_city_analytics
from load import save_bronze, save_silver, save_gold_daily, save_gold_city

def run_daily_pipeline():
    start = datetime.now()
    log_job_execution("START", "Daily Weather Pipeline")

    try:
        check_and_setup_schema()

        raw = extract_incremental()
        if raw is None:
            log_job_execution("SUCCESS", "No new data")
            return True

        clean = clean_and_enrich(raw)
        if clean is None:
            log_job_execution("ERROR", "Cleaning failed")
            return False

        daily = create_daily_summaries(clean)
        city = create_city_analytics(clean)

        save_bronze(raw) and save_silver(clean) and save_gold_daily(daily) and save_gold_city(city)

        duration = (datetime.now() - start).total_seconds()
        log_job_execution("SUCCESS", f"Completed in {duration:.1f}s")
        return True
    except Exception as e:
        log_job_execution("ERROR", str(e))
        return False

def run_initial_load():
    log_job_execution("START", "Initial Load")
    check_and_setup_schema()

    raw = extract_full(limit=5000)
    if not raw: return False

    clean = clean_and_enrich(raw)
    if not clean: return False

    daily = create_daily_summaries(clean)
    city = create_city_analytics(clean)

    raw.write.mode("overwrite").saveAsTable(config.BRONZE_TABLE)
    clean.write.mode("overwrite").saveAsTable(config.SILVER_TABLE)
    if daily: daily.write.mode("overwrite").saveAsTable(config.GOLD_DAILY_TABLE)
    if city: city.write.mode("overwrite").saveAsTable(config.GOLD_CITY_TABLE)

    log_job_execution("SUCCESS", "Initial load completed")
    return True

# Databricks job entry point
if __name__ == "__main__":
    run_daily_pipeline()