databricks_pipeline/
│
├── notebooks/
│   ├── 01_raw_ingest_autoloader.py      # Auto Loader: Ingest Parquet to Bronze
│   ├── 02_bronze_to_silver.py           # Clean & deduplicate
│   ├── 03_silver_to_gold_scd2.py        # SCD Type 2 logic
│   ├── 04_gold_aggregations.py          # Aggregation logic
│   ├── utils/
│   │   └── config.py                    # Paths, secrets, schema
│   │   └── helpers.py                   # Utility functions
│
├── configs/
│   └── schema.json                      # Optional: enforce schema
│
└── jobs/
    └── pipeline_job.json                # Databricks job config (optional)
