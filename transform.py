from pyspark.sql import functions as F
from config import config

def clean_and_enrich(raw_df):
    if raw_df is None:
        return None

    return (raw_df
            .filter(
                (F.col("temperature").isNotNull()) &
                (F.col("temperature").between(config.MIN_TEMPERATURE, config.MAX_TEMPERATURE)) &
                (F.col("humidity").between(0, 100)) &
                (F.col("city").isNotNull()) & (F.col("city") != "")
            )
            .withColumn("city_clean", F.upper(F.trim(F.col("city"))))
            .withColumn("temperature_f", (F.col("temperature") * 9/5) + 32)
            .withColumn("temp_category",
                F.when(F.col("temperature") < 0, "Freezing")
                 .when(F.col("temperature") < 10, "Cold")
                 .when(F.col("temperature") < 20, "Cool")
                 .when(F.col("temperature") < 30, "Warm")
                 .otherwise("Hot"))
            .withColumn("humidity_category",
                F.when(F.col("humidity") < 30, "Low")
                 .when(F.col("humidity") < 60, "Moderate")
                 .when(F.col("humidity") < 80, "High")
                 .otherwise("Very High"))
            .withColumn("quality_score",
                F.when((F.col("temperature").isNotNull()) &
                       (F.col("humidity").isNotNull()) &
                       (F.col("pressure").isNotNull()) &
                       (F.col("wind_speed").isNotNull()), 100).otherwise(75))
            .withColumn("processing_timestamp", F.current_timestamp())
            .withColumn("date_partition", F.to_date("created_at"))
            .dropDuplicates(["city", "created_at"]))