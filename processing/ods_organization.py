import datetime
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import types as ps_type
from delta.tables import DeltaTable

from spark_connect import get_spark
import components.schema_table as schema_tbl
import components.file_locale as locale
import components.object_name as objs

timestamp = datetime.datetime.now()
spark = get_spark()

def process_ods_organization() -> None:
    # TABLE 2 << ods / organziation
    source_table = DeltaTable.forPath(spark,locale.WAREHOUSE_ROOT_DIR + locale.table_stage["SBI_KVK"][2:])
    df_org = source_table.toDF()

    df_org = df_org.withColumn("create_date", F.col("modify_date").cast("timestamp")) \
        .groupBy("company_id") \
        .agg(F.max("modify_date").alias("create_date")) \
        .withColumn("active", F.lit(True)) \
        .withColumn("modify_date", F.lit(timestamp))
        
    # merge with ods
    target_table = DeltaTable.forPath(spark, locale.WAREHOUSE_ROOT_DIR + locale.table_ods["ORGANIZATION"][2:])
    merge_cond = 'target.company_id = source.company_id' 

    target_table.alias('target') \
        .merge(df_org.alias('source'), merge_cond) \
        .whenNotMatchedInsert(values =
                        {"company_id" : "source.company_id",
                            "active" : "source.active",
                            "create_date" : "source.create_date",
                            "modify_date" : "source.modify_date"
                        }
        ) \
        .whenNotMatchedBySourceUpdate(set =
                    {
                    "active"        : F.lit(False),
                    "modify_date"   : F.lit(timestamp)
                    }
        ) \
        .execute()

