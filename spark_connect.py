from functools import lru_cache

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

@lru_cache(maxsize=None)
def get_spark():
    """Be able to import spark_connect.py into other files to access SparkSession
    Use getOrCreate will reuse SparkSession if it has been created
    @lru_cache decorator will memoized f(x)
    """
    
    builder = SparkSession.builder.appName("kvk") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    return(configure_spark_with_delta_pip(builder).getOrCreate())