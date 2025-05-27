from spark_connect import get_spark
import components.schema_table as schema_tbl
import components.file_locale as locale
import components.object_name as objs
import components.file_info as ff

def load_flat_files(file_info: ff.file_info) -> None:
    """Load Flat Files into the raw tables
    different options based on file type
    do a preliminary drop of possible duplicate primary keys
    persist to the raw tables
    """

    if file_info.file_type == "csv":
        df = (get_spark().read.load(file_info.file_path,
                                    format=file_info.file_type,
                                    header=file_info.file_header,
                                    sep = file_info.file_sep,
                                    schema=file_info.file_schema)
            )
    if file_info.file_type == "json":
        df = (get_spark().read.load(file_info.file_path,
                                format = file_info.file_type,
                                inferSchema = "true"
                                ))
    
    df.dropDuplicates(file_info.primary_key)
    
    print(f'File_name: {file_info.output_table_name}')
    df.write.format("delta").mode("overwrite").saveAsTable(file_info.output_table_name)
    df = None


def build_raw_files() -> dict:
    """Build a dictionary with meta data about raw files to process"""
    raw_files = {}
    raw_files["SBI_DEFINITION"] = ff.file_info(file_path=locale.data_file["SBI"],
                                    file_schema=schema_tbl.schema_raw["SBI_DEFINITION"],
                                    primary_key=['CODE'],
                                    output_table_name=objs.table_raw["SBI_DEFINITION"],
                                    )

    raw_files["SBI_KVK"] = ff.file_info(file_path=locale.data_file["KVK"],
                                    file_schema=schema_tbl.schema_raw["SBI_KVK"],
                                    primary_key=['mutation_year','mutation_week','company_id','branch_id'],
                                    output_table_name=objs.table_raw["SBI_KVK"]
                                    )

    raw_files["SBI_PREDICTED"] = ff.file_info(file_path=locale.data_file["DATA_SCIENCE"],
                                    file_schema=schema_tbl.schema_raw["SBI_PREDICTED"],
                                    primary_key=['dossiernr','predictor_id'],
                                    output_table_name=objs.table_raw["SBI_PREDICTED"],
                                    )

    raw_files["SBI_MANUAL"] = ff.file_info(file_path=locale.data_file["MANUAL_SBI"],
                                    file_schema=schema_tbl.schema_raw["SBI_MANUAL"],
                                    file_type="json",
                                    primary_key=['company_id','last_modified_date'],
                                    output_table_name=objs.table_raw["SBI_MANUAL"]
                                    )
    

def procces_raw_files() -> bool:
    # load the data files and land them into the raw tables
    raw_files = build_raw_files()
    data_files = list(raw_files)
    for data_file in data_files:
        load_flat_files(raw_files[data_file])

    raw_files = None
    return(True)

