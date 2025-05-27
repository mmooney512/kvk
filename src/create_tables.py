from delta.tables import DeltaTable

from spark_connect import get_spark
import components.schema_table as schema_tbl
import components.file_locale as locale
import components.object_name as objs

spark = get_spark()

def build_raw_tables(tbl_name: str) -> None:
    DeltaTable.createIfNotExists(spark) \
        .location(locale.table_raw[tbl_name]) \
        .addColumns(schema_tbl.schema_raw[tbl_name]) \
        .tableName(objs.table_raw[tbl_name]) \
        .execute()

def build_stage_tables(tbl_name: str) -> None:
    DeltaTable.createIfNotExists(spark) \
        .location(locale.table_stage[tbl_name]) \
        .addColumns(schema_tbl.schema_stage[tbl_name]) \
        .tableName(objs.table_stage[tbl_name]) \
        .execute()
    
def build_ods_tables(tbl_name: str) -> None:
    DeltaTable.createIfNotExists(spark) \
        .location(locale.table_ods[tbl_name]) \
        .addColumns(schema_tbl.schema_ods[tbl_name]) \
        .tableName(objs.table_ods[tbl_name]) \
        .execute()    

def create_tables() -> bool:
    """build all the needed persistent tables""" 
    raw_tables = list(objs.table_raw)
    for tbl in raw_tables:
        build_raw_tables(tbl)

    stage_tables = list(objs.table_stage)
    for tbl in stage_tables:
        build_stage_tables(tbl)    

    ods_tables = list(objs.table_ods)
    for tbl in ods_tables:
        build_ods_tables(tbl)

    return(True)