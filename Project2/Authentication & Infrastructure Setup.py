# -*- coding: utf-8 -*-
# Databricks notebook source
"""
Azure Databricks Production Pipeline - Part 1/4
Authentication & Infrastructure Setup

Features:
1. Key Vault-backed secret scope for credentials
2. Service Principal ADLS Gen2 access
3. Storage mounts with error handling
4. Delta Lake configuration
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Key Vault Secret Scope Configuration

# COMMAND ----------

def create_kv_secret_scope(kv_name: str, scope_name: str, dbutils: object) -> bool:
    """
    Creates Key Vault-backed secret scope if it doesn't exist
    
    Args:
        kv_name: Azure Key Vault name (e.g., 'prod-kv-data-01')
        scope_name: Databricks secret scope name (e.g., 'data-lake-scope')
        dbutils: Databricks utilities object
        
    Returns:
        bool: True if scope created/exists, False on failure
        
    Production Notes:
        - Requires 'Key Vault Secrets Officer' permission on KV
        - Scope creation is idempotent
    """
    try:
        existing_scopes = [s.name for s in dbutils.secrets.listScopes()]
        if scope_name not in existing_scopes:
            dbutils.secrets.createScope(
                name=scope_name,
                scope_backend_type="AZURE_KEYVAULT",
                keyvault_uri=f"https://{kv_name}.vault.azure.net"
            )
            print(f"Secret scope '{scope_name}' created successfully")
        else:
            print(f"Scope '{scope_name}' already exists")
        return True
    except Exception as e:
        print(f"ERROR creating secret scope: {str(e)}")
        return False

# Example usage:
create_kv_secret_scope(
    kv_name="prod-kv-data-01", 
    scope_name="data-lake-scope",
    dbutils=dbutils
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Service Principal Authentication

# COMMAND ----------

from pyspark.sql import SparkSession
from typing import Dict

def get_service_principal_credentials(scope_name: str, dbutils: object) -> Dict[str, str]:
    """
    Retrieves Service Principal credentials from secret scope
    
    Args:
        scope_name: Databricks secret scope name
        dbutils: Databricks utilities object
        
    Returns:
        Dict with SPN credentials
        
    Production Notes:
        - Secrets should be pre-stored in Key Vault:
            - 'adls-spn-client-id'
            - 'adls-spn-client-secret'
            - 'adls-spn-tenant-id'
    """
    try:
        return {
            "client_id": dbutils.secrets.get(scope=scope_name, key="adls-spn-client-id"),
            "client_secret": dbutils.secrets.get(scope=scope_name, key="adls-spn-client-secret"),
            "tenant_id": dbutils.secrets.get(scope=scope_name, key="adls-spn-tenant-id")
        }
    except Exception as e:
        raise ValueError(f"Failed to retrieve SPN credentials: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 ADLS Gen2 Mount Points

# COMMAND ----------

def mount_adls_container(
    container_name: str,
    storage_account: str,
    mount_point: str,
    spn_credentials: Dict[str, str],
    dbutils: object
) -> bool:
    """
    Mounts ADLS Gen2 container using Service Principal
    
    Args:
        container_name: ADLS container name
        storage_account: Storage account name
        mount_point: Databricks mount path (e.g., '/mnt/raw')
        spn_credentials: Service Principal credentials dict
        dbutils: Databricks utilities object
        
    Returns:
        bool: True if mount successful/exists
        
    Production Notes:
        - Implements idempotent mount operation
        - Includes proper error handling
    """
    try:
        # Check if already mounted
        existing_mounts = [m.mountPoint for m in dbutils.fs.mounts()]
        if mount_point in existing_mounts:
            print(f"Mount point '{mount_point}' already exists")
            return True
        
        # OAuth configuration
        configs = {
            "fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": spn_credentials["client_id"],
            "fs.azure.account.oauth2.client.secret": spn_credentials["client_secret"],
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{spn_credentials['tenant_id']}/oauth2/token"
        }
        
        # Create mount
        dbutils.fs.mount(
            source=f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/",
            mount_point=mount_point,
            extra_configs=configs
        )
        
        print(f"Successfully mounted '{container_name}' to {mount_point}")
        return True
        
    except Exception as e:
        if "Directory already mounted" in str(e):
            return True
        print(f"ERROR mounting container: {str(e)}")
        return False

# Example usage:
spn_creds = get_service_principal_credentials("data-lake-scope", dbutils)

mount_adls_container(
    container_name="raw",
    storage_account="prodadlsgen2",
    mount_point="/mnt/raw",
    spn_credentials=spn_creds,
    dbutils=dbutils
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Delta Lake Configuration

# COMMAND ----------

def configure_delta_settings(spark: SparkSession) -> None:
    """
    Configures Delta Lake settings for production environment
    
    Args:
        spark: Active Spark session
        
    Production Notes:
        - Enables Change Data Feed for CDC
        - Sets optimal file sizes
        - Configures retention periods
    """
    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
    
    # Enable Change Data Feed for CDC
    spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
    
    # Optimize file sizes
    spark.conf.set("spark.databricks.delta.optimize.maxFileSize", "134217728")  # 128MB
    spark.conf.set("spark.databricks.delta.optimize.minFileSize", "67108864")   # 64MB
    
    print("Delta Lake production configuration applied")

# Initialize configuration
configure_delta_settings(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 Validation Checks

# COMMAND ----------

def validate_environment(dbutils: object, expected_mounts: list) -> bool:
    """
    Validates the setup environment
    
    Args:
        dbutils: Databricks utilities object
        expected_mounts: List of mount points to verify
        
    Returns:
        bool: True if all checks pass
    """
    try:
        # Verify mounts
        existing_mounts = [m.mountPoint for m in dbutils.fs.mounts()]
        missing_mounts = [m for m in expected_mounts if m not in existing_mounts]
        
        if missing_mounts:
            raise ValueError(f"Missing mounts: {missing_mounts}")
        
        # Verify Delta config
        if not spark.conf.get("spark.databricks.delta.properties.defaults.enableChangeDataFeed") == "true":
            raise ValueError("Change Data Feed not enabled")
            
        print("Environment validation passed")
        return True
        
    except Exception as e:
        print(f"Validation failed: {str(e)}")
        return False

# Run validation
validate_environment(
    dbutils=dbutils,
    expected_mounts=["/mnt/raw", "/mnt/bronze", "/mnt/silver", "/mnt/gold"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Next Steps:**
# MAGIC 1. Store your Service Principal credentials in Key Vault:
# MAGIC    - `adls-spn-client-id`
# MAGIC    - `adls-spn-client-secret`
# MAGIC    - `adls-spn-tenant-id`
# MAGIC 2. Create ADLS containers: raw, bronze, silver, gold
# MAGIC 3. Adjust storage account names in mount function calls
