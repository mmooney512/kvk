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

def process_ods_branch_kvk() -> None:
    # TABLE 5 << ods / sbi_assigned
    source_table = DeltaTable.forPath(spark,locale.WAREHOUSE_ROOT_DIR + locale.table_stage["SBI_KVK"][2:])
    df_sbi = source_table.toDF()


    df_sbi = df_sbi.withColumn("sbi_info", F.explode(
        F.array(
            F.array(F.lit('primary'), F.col("sbi_kvk_primary")),
            F.array(F.lit('secondary'), F.col("sbi_kvk_secondary")),
            F.array(F.lit('tertiary'), F.col("sbi_kvk_tertiary")),
            )
        )) \
        .withColumn("sbi_level", F.col("sbi_info")[0]) \
        .withColumn("sbi_id", F.col("sbi_info")[1]) \
        .withColumn("sbi_source", F.lit("kvk")) \
        .withColumn("active", F.lit(True)) \
        .withColumn("create_date", F.lit(timestamp)) \
        .withColumn("modify_date", F.lit(timestamp)) \
        .select("sbi_id", "company_id","sbi_source", "sbi_level", "active", "create_date", "modify_date") \
        .dropna(how="any")

    # merge with ODS
    target_table = DeltaTable.forPath(spark, locale.WAREHOUSE_ROOT_DIR + locale.table_ods["SBI_ASSIGNED"][2:])
    merge_cond = 'target.sbi_id = source.sbi_id AND target.company_id = source.company_id AND target.sbi_source = source.sbi_source'

    target_table.alias('target') \
        .merge(df_sbi.alias('source'), merge_cond) \
        .whenMatchedUpdate(set =
                        {"sbi_id"       : "source.sbi_id",
                        "company_id"    : "source.company_id",
                        "sbi_source"    : "source.sbi_source",
                        "sbi_level"     : "source.sbi_level",
                        "active"        : "source.active",
                        "modify_date"   : "source.modify_date"
                        }
        ) \
        .whenNotMatchedInsert(values =
                        {"sbi_id"       : "source.sbi_id",
                        "company_id"    : "source.company_id",
                        "sbi_source"    : "source.sbi_source",
                        "sbi_level"     : "source.sbi_level",
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

    source_table = None
    target_table = None
    df_sbi = None



def process_ods_sbi_manual() -> None:
    # TABLE 5 << ods / sbi_assigned
    source_table = DeltaTable.forPath(spark,locale.WAREHOUSE_ROOT_DIR + locale.table_stage["SBI_MANUAL"][2:])
    df_sbi = source_table.toDF()


    df_sbi = df_sbi.withColumn("sbi_info", F.explode(
        F.array(
            F.array(F.lit('primary'), F.col("manual_kvk_primary")),
            F.array(F.lit('secondary'), F.col("manual_kvk_secondary")),
            F.array(F.lit('tertiary'), F.col("manual_kvk_tertiary")),
            )
        )) \
        .withColumn("sbi_level", F.col("sbi_info")[0]) \
        .withColumn("sbi_id", F.col("sbi_info")[1]) \
        .withColumn("sbi_source", F.lit("manual")) \
        .withColumn("active", F.lit(True)) \
        .withColumn("create_date", F.col("last_modified_date")) \
        .withColumn("modify_date", F.lit(timestamp)) \
        .select("sbi_id", "company_id","sbi_source", "sbi_level", "active", "create_date", "modify_date") \
        .dropna(how="any")

    # merge with ODS
    target_table = DeltaTable.forPath(spark, locale.WAREHOUSE_ROOT_DIR + locale.table_ods["SBI_ASSIGNED"][2:])
    merge_cond = 'target.sbi_id = source.sbi_id AND target.company_id = source.company_id AND target.sbi_source = source.sbi_source'

    target_table.alias('target') \
        .merge(df_sbi.alias('source'), merge_cond) \
        .whenMatchedUpdate(set =
                        {"sbi_id"       : "source.sbi_id",
                        "company_id"    : "source.company_id",
                        "sbi_source"    : "source.sbi_source",
                        "sbi_level"     : "source.sbi_level",
                        "active"        : "source.active",
                        "modify_date"   : "source.modify_date"
                        }
        ) \
        .whenNotMatchedInsert(values =
                        {"sbi_id"       : "source.sbi_id",
                        "company_id"    : "source.company_id",
                        "sbi_source"    : "source.sbi_source",
                        "sbi_level"     : "source.sbi_level",
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

    source_table = None
    target_table = None
    df_sbi = None



def process_ods_branch_predicted() -> None:
    # TABLE 5 << ods / sbi_assigned
    # assign the level based on the higher probability highest == 1
    source_table = DeltaTable.forPath(spark,locale.WAREHOUSE_ROOT_DIR + locale.table_stage["SBI_PREDICTED"][2:])
    df_sbi = source_table.toDF()
    df_sbi = df_sbi.withColumn("sbi_level", F.dense_rank().over(Window.partitionBy("company_id").orderBy(F.desc("sbi_predicted_probability")))) \
        .withColumn("sbi_source", F.lit("predicted")) \
        .withColumn("active", F.lit(True)) \
        .withColumn("create_date", F.lit(timestamp)) \
        .withColumn("modify_date", F.lit(timestamp))
   

    # merge with ODS
    target_table = DeltaTable.forPath(spark, locale.WAREHOUSE_ROOT_DIR + locale.table_ods["SBI_ASSIGNED"][2:])
    merge_cond = 'target.sbi_id = source.sbi_predicted AND target.company_id = source.company_id AND target.sbi_source = source.sbi_source'

    target_table.alias('target') \
        .merge(df_sbi.alias('source'), merge_cond) \
        .whenMatchedUpdate(set =
                        {"sbi_id"       : "source.sbi_predicted",
                        "company_id"    : "source.company_id",
                        "sbi_source"    : "source.sbi_source",
                        "sbi_level"     : "source.sbi_level",
                        "active"        : "source.active",
                        "modify_date"   : "source.modify_date"
                        }
        ) \
        .whenNotMatchedInsert(values =
                        {"sbi_id"       : "source.sbi_predicted",
                        "company_id"    : "source.company_id",
                        "sbi_source"    : "source.sbi_source",
                        "sbi_level"     : "source.sbi_level",
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

    source_table = None
    target_table = None
    df_sbi = None

