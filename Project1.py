# Databricks notebook source
# COMMAND ----------
"""
Azure Databricks Data Pipeline with Delta Lake CDC
Purpose: Incrementally load data from source to ADLS Gen2 using service principal authentication
         with proper data layering (raw, bronze, silver, gold)

Requirements:
1. Azure Key Vault integrated with Databricks secret scope
2. Service principal with proper RBAC on ADLS Gen2
3. Delta Lake tables initialized in target locations
"""

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1. Configuration Setup

# COMMAND ----------
# DBTITLE 1,Initialize Configuration
# Retrieve all credentials from Databricks secret scope (linked to Azure Key Vault)
# Note: Secret scope must be created beforehand linking to your Azure Key Vault

# Service Principal credentials for ADLS Gen2 access
tenant_id = dbutils.secrets.get(scope="kv-secrets", key="tenant-id")
client_id = dbutils.secrets.get(scope="kv-secrets", key="client-id")
client_secret = dbutils.secrets.get(scope="kv-secrets", key="client-secret")

# ADLS Gen2 configuration
storage_account = dbutils.secrets.get(scope="kv-secrets", key="storage-account")
container_name = dbutils.secrets.get(scope="kv-secrets", key="container-name")

# Source database configuration (example: SQL Server)
source_jdbc_url = dbutils.secrets.get(scope="kv-secrets", key="source-jdbc-url")
source_user = dbutils.secrets.get(scope="kv-secrets", key="source-user")
source_password = dbutils.secrets.get(scope="kv-secrets", key="source-password")

# Target paths in ADLS Gen2
raw_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/raw"
bronze_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/bronze"
silver_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/silver"
gold_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/gold"

# Source table and primary key (configurable per table)
source_table = "your_source_table"  # Replace with your source table
primary_key = "id"  # Replace with your primary key column
watermark_column = "last_updated"  # Column to track changes (should be timestamp)

# COMMAND ----------
# DBTITLE 1,Set Spark Config for Service Principal Authentication
# Configure Spark to use Service Principal for ADLS Gen2 access
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2. Helper Functions

# COMMAND ----------
# DBTITLE 1,Define Utility Functions
from pyspark.sql.functions import col, lit, current_timestamp
from delta.tables import DeltaTable
import datetime

def get_last_processed_timestamp(table_name):
    """
    Get the last processed timestamp from control table
    Creates control table if it doesn't exist
    """
    control_table_path = f"{bronze_path}/_control_table"
    
    try:
        # Try to read existing control table
        control_df = spark.read.format("delta").load(control_table_path)
        last_timestamp = control_df.filter(col("table_name") == table_name) \
                                  .select("last_processed_timestamp") \
                                  .collect()[0][0]
        return last_timestamp
    except:
        # Control table doesn't exist, create it
        schema = "table_name STRING, last_processed_timestamp TIMESTAMP, processed_at TIMESTAMP"
        spark.createDataFrame([], schema) \
             .write.format("delta") \
             .mode("overwrite") \
             .save(control_table_path)
        
        # Initialize with minimum timestamp
        min_timestamp = datetime.datetime(1900, 1, 1)
        new_record = spark.createDataFrame(
            [(table_name, min_timestamp, current_timestamp())],
            ["table_name", "last_processed_timestamp", "processed_at"]
        )
        new_record.write.format("delta").mode("append").save(control_table_path)
        return min_timestamp

def update_last_processed_timestamp(table_name, new_timestamp):
    """
    Update the last processed timestamp in control table
    """
    control_table_path = f"{bronze_path}/_control_table"
    control_table = DeltaTable.forPath(spark, control_table_path)
    
    control_table.alias("target").merge(
        spark.createDataFrame([(table_name, new_timestamp, current_timestamp())],
                            ["table_name", "last_processed_timestamp", "processed_at"]).alias("source"),
        "target.table_name = source.table_name"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3. Data Loading Pipeline

# COMMAND ----------
# DBTITLE 1,Raw Layer: Extract New Data from Source
"""
Raw Layer Purpose:
- Capture data exactly as it exists in source system
- Preserve full history without transformation
- Append-only pattern
"""

# Get the last processed timestamp
last_processed = get_last_processed_timestamp(source_table)

# Read new/changed data from source
new_data_query = f"""
(SELECT *, {watermark_column} AS source_watermark 
 FROM {source_table} 
 WHERE {watermark_column} > '{last_processed}'
) as source_data
"""

new_data_df = spark.read \
    .format("jdbc") \
    .option("url", source_jdbc_url) \
    .option("dbtable", new_data_query) \
    .option("user", source_user) \
    .option("password", source_password) \
    .load()

# Get max watermark from current batch to update control table
if new_data_df.count() > 0:
    max_watermark = new_data_df.selectExpr(f"max(source_watermark)").collect()[0][0]
else:
    max_watermark = last_processed

# Write to raw layer (append mode to preserve history)
raw_table_path = f"{raw_path}/{source_table}"
new_data_df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(raw_table_path)

# Update control table with new watermark
update_last_processed_timestamp(source_table, max_watermark)

# COMMAND ----------
# DBTITLE 1,Bronze Layer: Initial Quality Checks
"""
Bronze Layer Purpose:
- Add technical metadata
- Basic data quality checks
- Still contains all source data but in structured format
"""

# Read from raw layer
raw_df = spark.read.format("delta").load(raw_table_path)

# Add technical columns and basic quality checks
bronze_df = raw_df.withColumn("ingestion_timestamp", current_timestamp()) \
                  .withColumn("is_valid", lit(True))  # Placeholder for actual validation logic

# Write to bronze layer (merge to handle updates)
bronze_table_path = f"{bronze_path}/{source_table}"

if DeltaTable.isDeltaTable(spark, bronze_table_path):
    # Existing table - perform merge
    bronze_table = DeltaTable.forPath(spark, bronze_table_path)
    
    # Merge condition - update if primary key exists, otherwise insert
    merge_condition = " AND ".join([f"target.{primary_key} = source.{primary_key}"])
    
    bronze_table.alias("target").merge(
        bronze_df.alias("source"),
        merge_condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    # First run - create table
    bronze_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(bronze_table_path)

# COMMAND ----------
# DBTITLE 1,Silver Layer: Cleaned and Standardized Data
"""
Silver Layer Purpose:
- Cleaned, standardized data
- Business entities rather than source tables
- Data quality enforced
"""

# Read from bronze layer
bronze_df = spark.read.format("delta").load(bronze_table_path)

# Apply transformations - this will vary by use case
silver_df = bronze_df.filter(col("is_valid") == True) \
                     .drop("is_valid")  # Example transformation

# Write to silver layer (SCD Type 1 - overwrite changes)
silver_table_path = f"{silver_path}/{source_table}"

if DeltaTable.isDeltaTable(spark, silver_table_path):
    silver_table = DeltaTable.forPath(spark, silver_table_path)
    
    merge_condition = " AND ".join([f"target.{primary_key} = source.{primary_key}"])
    
    silver_table.alias("target").merge(
        silver_df.alias("source"),
        merge_condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(silver_table_path)

# COMMAND ----------
# DBTITLE 1,Gold Layer: Business Aggregations
"""
Gold Layer Purpose:
- Business-ready aggregates and metrics
- Optimized for read performance
- May combine multiple silver tables
"""

# Read from silver layer
silver_df = spark.read.format("delta").load(silver_table_path)

# Example aggregation - this will vary by use case
gold_df = silver_df.groupBy("some_dimension_column") \
                   .agg({"some_measure_column": "avg", 
                         "other_measure": "sum"}) \
                   .withColumnRenamed("avg(some_measure_column)", "avg_measure") \
                   .withColumnRenamed("sum(other_measure)", "total_measure")

# Write to gold layer (full refresh for aggregates in this example)
gold_table_path = f"{gold_path}/{source_table}_aggregated"
gold_df.write.format("delta").mode("overwrite").save(gold_table_path)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4. Maintenance Operations

# COMMAND ----------
# DBTITLE 1,Optimize Delta Tables
# Optimize all tables for performance
for table_path in [bronze_table_path, silver_table_path, gold_table_path]:
    spark.sql(f"OPTIMIZE delta.`{table_path}`")

# COMMAND ----------
# DBTITLE 1,Vacuum Old Files
# Clean up old files (retain 7 days of history)
for table_path in [bronze_table_path, silver_table_path, gold_table_path]:
    spark.sql(f"VACUUM delta.`{table_path}` RETAIN 168 HOURS")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5. Logging and Monitoring

# COMMAND ----------
# DBTITLE 1,Log Pipeline Execution
# Log pipeline execution details
pipeline_log = {
    "pipeline_run_time": str(datetime.datetime.now()),
    "source_table": source_table,
    "records_processed": new_data_df.count(),
    "last_watermark": str(max_watermark),
    "status": "SUCCESS"
}

# Convert to DF and write to logs table
log_df = spark.createDataFrame([pipeline_log])
log_table_path = f"{bronze_path}/_pipeline_logs"

log_df.write \
    .format("delta") \
    .mode("append") \
    .save(log_table_path)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pipeline Execution Complete
