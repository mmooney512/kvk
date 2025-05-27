import datetime
from spark_connect import get_spark
from pyspark.sql import types as ps_type

today = datetime.date.today()
timestamp = datetime.datetime.now()
spark = get_spark()

def init_data_language():
    lang_schema = ps_type.StructType([
    ps_type.StructField("language_id", ps_type.StringType(), True),
    ps_type.StructField("language_name", ps_type.StringType(), True),
    ps_type.StructField("active", ps_type.BooleanType(), True),
    ps_type.StructField("create_date", ps_type.TimestampType(), True),
    ps_type.StructField("modify_date", ps_type.TimestampType(), True),
])

    return(spark.createDataFrame([
        (0,"Dutch",True,timestamp,timestamp),
        (1,"English",True,timestamp,timestamp),
        (2,"German",False,timestamp,timestamp),
        ],schema=lang_schema)
    )