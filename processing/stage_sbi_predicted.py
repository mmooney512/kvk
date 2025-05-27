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

def process_sbi_predicted() -> None:
    # TABLE 2   >> STAGE_SBI_PREDICTED
    # start converting the data frame
    # actions:   
    #   drop rows where probability < 0,80
    #   find the most recent model for company
    #   use a 2nd filter since, the most recent model may have a lower probability than prior model

    source_table = DeltaTable.forPath(spark,locale.WAREHOUSE_ROOT_DIR + locale.table_raw["SBI_PREDICTED"][2:])
    df_source_predicted = source_table.toDF()


    df_source_predicted = df_source_predicted.withColumn("modify_date", F.lit(today)) \
        .filter(F.col("probability") >= 0.80) \
        .withColumn("max_model", F.max("predictor_id").over(Window.partitionBy("dossiernr"))) \
        .filter(F.col("max_model") == F.col("predictor_id")) \
        .drop("max_model")


    # merge with staging
    target_table = DeltaTable.forPath(spark, locale.WAREHOUSE_ROOT_DIR + locale.table_stage["SBI_PREDICTED"][2:])
    merge_cond = 'target.company_id = source.dossiernr AND target.sbi_predicted = source.sector_code'

    target_table.alias('target') \
        .merge(df_source_predicted.alias('source'), merge_cond) \
        .whenMatchedUpdate(set =
                        {"company_id" : "source.dossiernr",
                        "sbi_predicted" : "source.sector_code",
                        "sbi_predicted_probability" : "source.probability",
                        "sbi_predicted_version" : "source.predictor_id",
                        "modify_date" : "source.modify_date"
                        }
        ) \
        .whenNotMatchedInsert(values =
                        {"company_id" : "source.dossiernr",
                        "sbi_predicted" : "source.sector_code",
                        "sbi_predicted_probability" : "source.probability",
                        "sbi_predicted_version" : "source.predictor_id",
                        "modify_date" : "source.modify_date"
                        }
        ) \
        .whenNotMatchedBySourceDelete() \
        .execute()

    source_table = None
    df_source_predicted = None

