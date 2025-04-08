from pyspark.sql.functions import input_file_name
from utils.config import *

spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint",
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .option("cloudFiles.schemaLocation", bronze_checkpoint)
      .load(raw_path)
      .withColumn("source_file", input_file_name()))

(df.writeStream
   .format("delta")
   .option("checkpointLocation", bronze_checkpoint)
   .outputMode("append")
   .start(bronze_path))
