# config.py

# Azure ADLS Gen2 path setup
container_name = "raw"
storage_account = "yourstorageaccount"
raw_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/data"

# Output paths
bronze_path = "/mnt/datalake/bronze/table_name"
silver_path = "/mnt/datalake/silver/table_name"
gold_path   = "/mnt/datalake/gold/table_name"

# Checkpoint locations
bronze_checkpoint = "/mnt/checkpoints/bronze"
silver_checkpoint = "/mnt/checkpoints/silver"

# Table names
bronze_table = "db.bronze_table"
silver_table = "db.silver_table"
gold_table   = "db.gold_table"

# Service Principal secrets (set via Databricks secret scope)
client_id = dbutils.secrets.get("sp-scope", "client-id")
tenant_id = dbutils.secrets.get("sp-scope", "tenant-id")
client_secret = dbutils.secrets.get("sp-scope", "client-secret")
