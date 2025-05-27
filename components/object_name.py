# object names for tables
# for simplicity will list as CONST; 

# ----------------------------------------------------------------------------
# tables
# ----------------------------------------------------------------------------
# RAW TABLES
table_raw ={
    "SBI_DEFINITION"      : "raw_sbi_definition",
    "SBI_KVK"             : "raw_kvk_mutation",
    "SBI_PREDICTED"       : "raw_datascience_sbi",
    "SBI_MANUAL"          : "raw_human_annotated_sbi",
}


# STAGING TABLES
table_stage ={
    "SBI_DEFINITION"      : "stage_sbi_definition",
    "SBI_KVK"             : "stage_sbi_kvk",
    "SBI_PREDICTED"       : "stage_sbi_predicted",
    "SBI_MANUAL"          : "stage_sbi_manual",
}


# ODS TABLES
table_ods ={
    "SBI_DESCRIPTION"    : "ods_sbi_description",
    "SBI_ASSIGNED"       : "ods_sbi_assigned",
    "LANGUAGE"           : "ods_language",
    "ORGANIZATION"       : "ods_organization",
    "BRANCH"             : "ods_branch"
}
