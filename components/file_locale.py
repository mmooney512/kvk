# FILE LOCATIONS
# For simplicity sake just putting into dictiionary, in real world, coming via API, stream, etc..
WAREHOUSE_ROOT_DIR = "./spark-warehouse/"

data_file ={
    "SBI"           :"./data/sbi_definitions.csv",
    "DATA_SCIENCE"  :"./data/datascience_sbis.csv",
    "KVK"           :"./data/kvk_mutations.csv",
    "MANUAL_SBI"    :"./data/human_annotated_sbis.jsonl",
}

# RAW TABLES
table_raw ={
    "SBI_DEFINITION"      : "./raw/sbi_definition",
    "SBI_KVK"             : "./raw/kvk_mutation",
    "SBI_PREDICTED"       : "./raw/datascience_sbi",
    "SBI_MANUAL"          : "./raw/human_annotated_sbi",
}




# STAGING TABLES
table_stage ={
    "SBI_DEFINITION"    : "./stage/stage_sbi_definition",
    "SBI_KVK"           : "./stage/stage_sbi_kvk",
    "SBI_PREDICTED"     : "./stage/stage_sbi_predicted",
    "SBI_MANUAL"        : "./stage/stage_sbi_manual",
}


# ODS TABLES
table_ods ={
    "SBI_DESCRIPTION"    : "./ods/sbi_description",
    "SBI_ASSIGNED"       : "./ods/sbi_assigned",
    "LANGUAGE"           : "./ods/language",
    "ORGANIZATION"       : "./ods/organization",
    "BRANCH"             : "./ods/branch",
}