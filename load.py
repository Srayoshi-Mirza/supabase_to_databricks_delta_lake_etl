from config import config

def save_bronze(df):
    if df is None: return False
    df.write.mode("append").option("mergeSchema", "true").saveAsTable(config.BRONZE_TABLE)
    return True

def save_silver(df):
    if df is None: return False
    df.write.mode("append").option("mergeSchema", "true").saveAsTable(config.SILVER_TABLE)
    return True

def save_gold_daily(df):
    if df is None: return False
    df.write.mode("append").option("mergeSchema", "true").saveAsTable(config.GOLD_DAILY_TABLE)
    return True

def save_gold_city(df):
    if df is None: return False
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(config.GOLD_CITY_TABLE)
    return True