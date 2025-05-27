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
import components.file_info as ff

today = datetime.date.today()
spark = get_spark()

def process_sbi_kvk() -> None:
    # TABLE 3   >> stage_sbi_kvk
    # start converting the data frame
    # actions:   
    #   convert date_column year + week to a date
    #   group and only keep the most recent update
    
    source_table = DeltaTable.forPath(spark,locale.WAREHOUSE_ROOT_DIR + locale.table_raw["SBI_KVK"][2:])
    df_source_kvk = source_table.toDF()

    # add date_column year + week, default to first day of week
    # Date Format "yyyyw" is invalid because the "w" has been removed from date format
    df_source_kvk = df_source_kvk.withColumn("modified_date", F.date_add(F.to_date("mutation_year","yyyy"), (F.col("mutation_week")-1)*7))

    # only keep the most recent update
    window = Window.partitionBy(["company_id","branch_id"])
    df_source_kvk = df_source_kvk.withColumn("max_date", F.max("modified_date").over(window)) \
        .filter(F.col("max_date") == F.col("modified_date")) \
        .drop("max_date")


    # merge with staging
    target_table = DeltaTable.forPath(spark, locale.WAREHOUSE_ROOT_DIR + locale.table_stage["SBI_KVK"][2:])
    merge_cond = 'target.company_id = source.company_id AND target.branch_id = source.branch_id'

    target_table.alias('target') \
        .merge(df_source_kvk.alias('source'), merge_cond) \
        .whenMatchedUpdate(set =
                        {"company_id" : "source.company_id",
                        "branch_id" : "source.branch_id",
                        "sbi_kvk_primary" : "source.SBIHOOFDACT",
                        "sbi_kvk_secondary" : "source.SBINEVENACT1",
                        "sbi_kvk_tertiary" : "source.SBINEVENACT2",
                        "modify_date" : "source.modified_date"
                        }
        ) \
        .whenNotMatchedInsert(values =
                        {"company_id" : "source.company_id",
                        "branch_id" : "source.branch_id",
                        "sbi_kvk_primary" : "source.SBIHOOFDACT",
                        "sbi_kvk_secondary" : "source.SBINEVENACT1",
                        "sbi_kvk_tertiary" : "source.SBINEVENACT2",
                        "modify_date" : "source.modified_date"
                        }
        ) \
        .execute()

    source_table = None
    df_source_kvk = None

process_sbi_kvk()