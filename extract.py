import requests
import pandas as pd
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from config import config

def get_headers():
    return {
        "apikey": config.SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {config.SUPABASE_ANON_KEY}",
        "Content-Type": "application/json"
    }

def extract_incremental():
    try:
        cutoff = (datetime.now() - timedelta(days=config.INCREMENTAL_DAYS)).isoformat()
        url = f"{config.SUPABASE_URL}/rest/v1/weather_data"
        params = {"select": "*", "created_at": f"gte.{cutoff}", "order": "created_at.desc"}

        resp = requests.get(url, headers=get_headers(), params=params, timeout=config.REQUEST_TIMEOUT)
        if resp.status_code != 200 or not resp.json():
            return None

        df = pd.DataFrame(resp.json())
        for col in ["created_at", "updated_at"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])

        spark_df = spark.createDataFrame(df)
        return (spark_df
                .withColumn("extraction_timestamp", F.current_timestamp())
                .withColumn("job_type", F.lit("daily_incremental")))
    except:
        return None

def extract_full(limit=5000):
    try:
        url = f"{config.SUPABASE_URL}/rest/v1/weather_data"
        params = {"select": "*", "order": "limit": str(limit)}

        resp = requests.get(url, headers=get_headers(), params=params, timeout=config.REQUEST_TIMEOUT)
        if resp.status_code != 200 or not resp.json():
            return None

        df = pd.DataFrame(resp.json())
        for col in ["created_at", "updated_at"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])

        spark_df = spark.createDataFrame(df)
        return (spark_df
                .withColumn("extraction_timestamp", F.current_timestamp())
                .withColumn("job_type", F.lit("full_load")))
    except:
        return None