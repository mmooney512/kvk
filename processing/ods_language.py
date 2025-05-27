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
import components.init_data as init_data

today = datetime.date.today()
timestamp = datetime.datetime.now()
spark = get_spark()

def process_ODS_language() -> None:
    # TABLE 1  << ods / language
    # do an init load if no data is present
    if DeltaTable.forPath(spark,locale.WAREHOUSE_ROOT_DIR + locale.table_ods["LANGUAGE"][2:]).toDF().count() < 1:
        df_language = init_data.init_data_language()
        df_language = df_language.withColumn("language_id", F.col("language_id").cast(dataType=ps_type.IntegerType())) 

        df_language.write.format("delta").mode("overwrite").saveAsTable(objs.table_ods["LANGUAGE"]) 
        df_language = None
        