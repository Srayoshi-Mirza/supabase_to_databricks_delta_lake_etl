from pyspark.sql import functions as F
from datetime import datetime, timedelta

class WeatherConfig:
    SUPABASE_URL = "supabase_url"
    SUPABASE_ANON_KEY = "your_Supabase_anon_key"

    SCHEMA_NAME = "weather_warehouse"
    INCREMENTAL_DAYS = 2
    REQUEST_TIMEOUT = 30
    MIN_TEMPERATURE = -50
    MAX_TEMPERATURE = 60

    BRONZE_TABLE = f"{SCHEMA_NAME}.bronze_weather_data"
    SILVER_TABLE = f"{SCHEMA_NAME}.silver_weather_clean"
    GOLD_DAILY_TABLE = f"{SCHEMA_NAME}.gold_daily_summaries"
    GOLD_CITY_TABLE = f"{SCHEMA_NAME}.gold_city_analytics"

    use_schema = True

config = WeatherConfig()