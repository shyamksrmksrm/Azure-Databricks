# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import uuid

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AutoLoaderWithSCDType2-ServicePrincipal") \
    .getOrCreate()

# Configuration parameters
storage_account_name = "yourstorageaccount"
source_container = "source-container"
target_container = "target-container"
source_path = f"abfss://{source_container}@{storage_account_name}.dfs.core.windows.net/source-path/"
target_path = f"abfss://{target_container}@{storage_account_name}.dfs.core.windows.net/target-path/"

# Service Principal credentials from Databricks secrets
client_id = dbutils.secrets.get(scope="kv-secrets", key="adls-sp-client-id")
tenant_id = dbutils.secrets.get(scope="kv-secrets", key="adls-sp-tenant-id")
client_secret = dbutils.secrets.get(scope="kv-secrets", key="adls-sp-client-secret")

# Set Service Principal credentials for ADLS Gen2 access
spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# Define schema evolution checkpoint location
checkpoint_path = f"{target_path}_checkpoints/"

# Define the base schema (optional, can be inferred)
base_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("status", StringType(), True),
    StructField("load_date", TimestampType(), True)  # Added for SCD tracking
])

# Function to generate surrogate keys
def generate_surrogate_key():
    return str(uuid.uuid4())

generate_surrogate_key_udf = udf(generate_surrogate_key, StringType())

# Read stream using AutoLoader with schema evolution
def read_stream_with_autoloader():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")  # or "csv", "json", etc.
        .option("cloudFiles.schemaLocation", checkpoint_path + "_schema")
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # or "addNewColumns", "failOnNewColumns"
        .option("cloudFiles.inferColumnTypes", "true")
        .option("mergeSchema", "true")
        .schema(base_schema)  # Optional base schema
        .load(source_path)
    )

# Function to handle SCD Type 2 logic with Delta Lake
def apply_scd_type_2_delta(micro_batch_df, batch_id):
    # Get current timestamp for this batch
    current_timestamp = datetime.now()
    
    # Generate surrogate keys for new records
    micro_batch_df = micro_batch_df.withColumn("surrogate_key", generate_surrogate_key_udf())
    
    # Add metadata columns for SCD Type 2
    micro_batch_df = micro_batch_df.withColumn("effective_date", lit(current_timestamp)) \
                                  .withColumn("expiry_date", lit(None).cast(TimestampType())) \
                                  .withColumn("is_current", lit(True)) \
                                  .withColumn("batch_id", lit(batch_id))
    
    # Create or get Delta table
    delta_table_path = f"{target_path}customers_delta/"
    
    # For initial run, create the Delta table if it doesn't exist
    if not DeltaTable.isDeltaTable(spark, delta_table_path):
        micro_batch_df.write \
            .format("delta") \
            .mode("append") \
            .save(delta_table_path)
        return
    
    # Get Delta table
    delta_table = DeltaTable.forPath(spark, delta_table_path)
    
    # Define the merge condition (match on natural key)
    merge_condition = "target.customer_id = source.customer_id AND target.is_current = true"
    
    # Define the update set for when records change
    update_set = {
        "expiry_date": "current_timestamp()",
        "is_current": "false",
        "batch_id": "source.batch_id"
    }
    
    # Define the insert set for new records
    insert_set = {
        "customer_id": "source.customer_id",
        "name": "source.name",
        "email": "source.email",
        "address": "source.address",
        "status": "source.status",
        "load_date": "source.load_date",
        "surrogate_key": "source.surrogate_key",
        "effective_date": "source.effective_date",
        "expiry_date": "source.expiry_date",
        "is_current": "source.is_current",
        "batch_id": "source.batch_id"
    }
    
    # Execute the merge operation
    (delta_table.alias("target")
        .merge(
            micro_batch_df.alias("source"),
            merge_condition
        )
        .whenMatchedUpdate(set = update_set)
        .whenNotMatchedInsert(values = insert_set)
        .execute()
    )

# Start the streaming job with Delta Lake SCD Type 2
streaming_query = (read_stream_with_autoloader()
    .writeStream
    .foreachBatch(apply_scd_type_2_delta)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)  # For testing, use .trigger(processingTime='10 seconds') in production
    .start()
)

# For production, you might want to await termination
streaming_query.awaitTermination()
