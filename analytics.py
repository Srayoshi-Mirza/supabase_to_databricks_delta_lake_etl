from pyspark.sql import functions as F

def create_daily_summaries(clean_df):
    if clean_df is None:
        return None
    return (clean_df
            .groupBy("city_clean", "date_partition")
            .agg(
                F.count("*").alias("reading_count"),
                F.avg("temperature").alias("avg_temperature"),
                F.min("temperature").alias("min_temperature"),
                F.max("temperature").alias("max_temperature"),
                F.avg("humidity").alias("avg_humidity"),
                F.avg("pressure").alias("avg_pressure"),
                F.avg("wind_speed").alias("avg_wind_speed"),
                F.first("weather_condition").alias("dominant_condition"),
                F.avg("quality_score").alias("avg_data_quality"),
                F.max("created_at").alias("latest_reading")
            )
            .withColumn("summary_date", F.current_date())
            .withColumn("created_by_job", F.lit("daily_weather_pipeline")))

def create_city_analytics(clean_df):
    if clean_df is None:
        return None
    return (clean_df
            .groupBy("city_clean")
            .agg(
                F.count("*").alias("total_readings"),
                F.avg("temperature").alias("avg_temperature"),
                F.stddev("temperature").alias("temp_volatility"),
                F.avg("humidity").alias("avg_humidity"),
                F.avg("pressure").alias("avg_pressure"),
                F.avg("wind_speed").alias("avg_wind_speed"),
                F.min("created_at").alias("first_reading"),
                F.max("created_at").alias("latest_reading"),
                F.avg("quality_score").alias("data_quality_score")
            )
            .withColumn("analysis_date", F.current_date())
            .withColumn("created_by_job", F.lit("daily_weather_pipeline")))