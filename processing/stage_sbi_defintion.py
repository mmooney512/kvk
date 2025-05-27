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


def process_sbi_defintion() -> None:
    # TABLE sbi_definitions
    # start converting the data frame
    # actions   trim text fields
    #           add modify date column
    #           drop duplicates

    source_table = DeltaTable.forPath(spark, locale.WAREHOUSE_ROOT_DIR + locale.table_raw["SBI_DEFINITION"][2:])
    df_source_sbi = source_table.toDF()
    df_source_sbi = df_source_sbi.filter((df_source_sbi["code"].isNotNull() & (df_source_sbi["status"]<=1))) \
        .withColumn("descr_short_nl", F.trim("descr_short_nl")) \
        .withColumn("descr_short_en", F.trim("descr_short_en")) \
        .withColumn("modify_date", F.lit(today))

    # need to drop rows with duplicate codes
    df_source_sbi = df_source_sbi.withColumn("count", F.count("code").over(Window.partitionBy("code"))) \
        .filter(F.col("count") == 1).drop("count")


    # merge with staging
    target_table = DeltaTable.forPath(spark, locale.WAREHOUSE_ROOT_DIR + locale.table_stage["SBI_DEFINITION"][2:])
    merge_cond = 'target.sbi_id = source.code'

    target_table.alias('target') \
        .merge(df_source_sbi.alias('source'), merge_cond) \
        .whenMatchedUpdate(set =
                        {"sbi_id" : "source.code",
                        "descr_short_nl" : "source.descr_short_nl",
                        "descr_short_en" : "source.descr_short_en",
                        "status" : "source.status",
                        "modify_date" : "source.modify_date"
                        }
        ) \
        .whenNotMatchedInsert(values =
                        {"sbi_id" : "source.code",
                        "descr_short_nl" : "source.descr_short_nl",
                        "descr_short_en" : "source.descr_short_en",
                        "status" : "source.status",
                        "modify_date" : "source.modify_date"
                        }
        ) \
        .execute()

    source_table = None
    df_source_sbi = None    
