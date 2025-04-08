from utils.config import *
from pyspark.sql.functions import col

df = (spark.readStream
      .format("delta")
      .load(bronze_path)
      .dropDuplicates(["primary_key", "event_timestamp"]))

(df.writeStream
   .format("delta")
   .option("checkpointLocation", silver_checkpoint)
   .outputMode("append")
   .start(silver_path))
