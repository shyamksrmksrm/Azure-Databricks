from utils.config import *
from pyspark.sql.functions import col, count, to_date

df = spark.read.format("delta").load(gold_path).filter("is_current = true")

daily_summary = (
    df.withColumn("event_date", to_date("event_timestamp"))
      .groupBy("event_date")
      .agg(count("*").alias("total_events"))
)

daily_summary.write.format("delta").mode("overwrite").save("/mnt/datalake/gold/daily_summary")
