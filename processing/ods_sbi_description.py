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

def process_ods_sbi_description() -> None:
    # TABLE 4 << ods / sbi_description
    source_table = DeltaTable.forPath(spark,locale.WAREHOUSE_ROOT_DIR + locale.table_stage["SBI_DEFINITION"][2:])
    df_org = source_table.toDF()


    df_org = df_org.withColumn("sbi_info", F.explode(
        F.array(
            F.array(F.lit('0'), F.col("descr_short_nl")),
            F.array(F.lit('1'), F.col("descr_short_en")),
        )
    )) \
    .withColumn("language_id", F.col("sbi_info")[0]) \
    .withColumn("sbi_short_description", F.col("sbi_info")[1]) \
    .withColumn("active", F.lit(True)) \
    .withColumn("create_date", F.lit(timestamp)) \
    .withColumn("modify_date", F.lit(timestamp)) \
    .select("sbi_id", "language_id","sbi_short_description", "active", "create_date", "modify_date") \
    .dropna(how="any")



    # merge with ods
    target_table = DeltaTable.forPath(spark, locale.WAREHOUSE_ROOT_DIR + locale.table_ods["SBI_DESCRIPTION"][2:])
    merge_cond = 'target.sbi_id = source.sbi_id' 

    target_table.alias('target') \
    .merge(df_org.alias('source'), merge_cond) \
    .whenMatchedUpdate(set =
                    {"sbi_id"       : "source.sbi_id",
                    "language_id"   : "source.language_id",
                    "sbi_short_description"    : "source.sbi_short_description",
                    "active"        : "source.active",
                    "modify_date"   : "source.modify_date"
                    }
    ) \
    .whenNotMatchedInsert(values =
                    {"sbi_id"       : "source.sbi_id",
                    "language_id"   : "source.language_id",
                    "sbi_short_description"    : "source.sbi_short_description",
                    "active"        : "source.active",
                    "create_date"   : "source.create_date",
                    "modify_date"   : "source.modify_date"
                    }
    ) \
    .whenNotMatchedBySourceUpdate(set =
                    {
                    "active"        : F.lit(False),
                    "modify_date"   : F.lit(timestamp)
                    }
    ) \
    .execute()

