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

def process_sbi_manual() -> None:
    # TABLE 4   >> stage_sbi_manaul
    # start converting the data frame
    # actions:   
    #   convert date_column year + week to a date
    #   group and only keep the most recent update    
    #   drop the intermediate columns
    #   only keep the most recent update
    #   drop if sbi is not specified

    source_table = DeltaTable.forPath(spark,locale.WAREHOUSE_ROOT_DIR + locale.table_raw["SBI_MANUAL"][2:])
    df_source_manual = source_table.toDF()

    # add date_column year + week, default to first day of week
    # Date Format "yyyyw" is invalid because the "w" has been removed from date format
    df_source_manual = df_source_manual.withColumn("modfiy_date", F.lit(today)) \
        .withColumn("last_modified_date_lng", F.col("last_modified_date").cast(dataType=ps_type.LongType())/1000) \
        .withColumn("last_modified_date_dt", F.to_date(F.col("last_modified_date_lng").cast(dataType=ps_type.TimestampType()))) \
        .withColumn("manual_kvk_primary", F.split(F.col("sbi_codes"), ";").getItem(0)) \
        .withColumn("manual_kvk_secondary", F.split(F.col("sbi_codes"), ";").getItem(1)) \
        .withColumn("manual_kvk_tertiary", F.split(F.col("sbi_codes"), ";").getItem(2)) \
        .drop("last_modified_date", "last_modified_date_lng")   


    # only keep the most recent update
    window = Window.partitionBy(["company_id"])
    df_source_manual = df_source_manual.withColumn("max_date", F.max("last_modified_date_dt").over(window)) \
        .filter(F.col("max_date") == F.col("last_modified_date_dt") & F.length(F.col("manual_kvk_primary")) >= 1) \
        .drop("max_date")


    print(df_source_manual.dtypes)

    # merge with staging
    target_table = DeltaTable.forPath(spark, locale.WAREHOUSE_ROOT_DIR + locale.table_stage["SBI_MANUAL"][2:])
    print(f'target: {locale.WAREHOUSE_ROOT_DIR + locale.table_stage["SBI_MANUAL"][2:]}')
    
    merge_cond = 'target.company_id = source.company_id'

    target_table.alias('target') \
        .merge(df_source_manual.alias('source'), merge_cond) \
        .whenMatchedUpdate(set =
                       {"company_id" : "source.company_id",
                        "last_modified_date" : "source.last_modified_date_dt",
                        "manual_kvk_primary" : "source.manual_kvk_primary",
                        "manual_kvk_secondary" : "source.manual_kvk_secondary",
                        "manual_kvk_tertiary" : "source.manual_kvk_tertiary",
                        #"modify_date" : "source.last_modified_date_dt"
                       }
        ) \
        .whenNotMatchedInsert(values =
                        {"company_id" : "source.company_id",
                        "last_modified_date" : "source.last_modified_date_dt",
                        "manual_kvk_primary" : "source.manual_kvk_primary",
                        "manual_kvk_secondary" : "source.manual_kvk_secondary",
                        "manual_kvk_tertiary" : "source.manual_kvk_tertiary",
                        }
        ) \
        .whenNotMatchedBySourceDelete() \
        .execute()
    
    #    .whenNotMatchedBySourceDelete() \
        

    source_table = None
    df_source_manual = None
