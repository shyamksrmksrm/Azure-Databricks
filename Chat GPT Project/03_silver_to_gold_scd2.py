from utils.config import *
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

incoming = spark.read.format("delta").load(silver_path)

gold_table = DeltaTable.forPath(spark, gold_path)

updates = (
    gold_table.alias("target")
    .merge(incoming.alias("source"), "target.id = source.id")
    .whenMatchedUpdate(condition="target.is_current = true AND target.hash != source.hash", 
                       set={"is_current": "false", "end_date": "current_timestamp()"})
    .whenNotMatchedInsert(values={
        "id": "source.id",
        "name": "source.name",
        "start_date": "current_timestamp()",
        "end_date": "null",
        "is_current": "true",
        "hash": "source.hash"
    })
)

updates.execute()
