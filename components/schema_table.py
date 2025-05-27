# SCHEMA for each of the flat file
# for simplicity will list as CONST; 
# however in production would store this in table
# job_name, flat_file_schema, last_modify_date, etc..

from pyspark.sql import types as ps_type

# ----------------------------------------------------------------------------
# raw files / tables
# ----------------------------------------------------------------------------
schema_raw = {
# FIRST FILE << sbi_definitions.csv
"SBI_DEFINITION"    : ps_type.StructType([
                    ps_type.StructField("code", ps_type.StringType(), True),
                    ps_type.StructField("descr_short_nl", ps_type.StringType(), True),
                    ps_type.StructField("descr_short_en", ps_type.StringType(), True),
                    ps_type.StructField("status", ps_type.IntegerType(), True),
                    ]),

# SECOND FILE << datascience_sbis.csv
"SBI_PREDICTED"     : ps_type.StructType([
                    ps_type.StructField("dossiernr", ps_type.StringType(), True),
                    ps_type.StructField("sector_code", ps_type.StringType(), True),
                    ps_type.StructField("probability", ps_type.DecimalType(7,6), True),
                    ps_type.StructField("predictor_id", ps_type.IntegerType(), True),
                    ]),

# THIRD FILE << kvk_mutations.csv
"SBI_KVK"           : ps_type.StructType([
                    ps_type.StructField("mutation_year", ps_type.IntegerType(), True),
                    ps_type.StructField("mutation_week", ps_type.IntegerType(), True),
                    ps_type.StructField("company_id", ps_type.StringType(), True),
                    ps_type.StructField("branch_id", ps_type.StringType(), True),
                    ps_type.StructField("SBIHOOFDACT", ps_type.StringType(), True),
                    ps_type.StructField("SBINEVENACT1", ps_type.StringType(), True),
                    ps_type.StructField("SBINEVENACT2", ps_type.StringType(), True),
                    ]),

# FOURTH FILE << human_annotated_sbis.jsonl
"SBI_MANUAL"        : ps_type.StructType([
                    ps_type.StructField("company_id", ps_type.StringType(), True),
                    ps_type.StructField("last_modified_date", ps_type.LongType(), True),
                    ps_type.StructField("sbi_codes", ps_type.StringType(), True),
                    ]),
}

# ----------------------------------------------------------------------------
# stage tables
# ----------------------------------------------------------------------------
schema_stage = {
    # FIRST TABLE << stage_sbi_definition
    "SBI_DEFINITION" : ps_type.StructType([
                        ps_type.StructField("sbi_id", ps_type.StringType(), True),
                        ps_type.StructField("descr_short_nl", ps_type.StringType(), True),
                        ps_type.StructField("descr_short_en", ps_type.StringType(), True),
                        ps_type.StructField("status", ps_type.IntegerType(), True),
                        ps_type.StructField("modify_date", ps_type.DateType(), True),
                        ]),    
    # THIRD FILE << kvk_mutations.csv
    "SBI_KVK"           : ps_type.StructType([
                        ps_type.StructField("company_id", ps_type.StringType(), True),
                        ps_type.StructField("branch_id", ps_type.StringType(), True),
                        ps_type.StructField("sbi_kvk_primary", ps_type.StringType(), True),
                        ps_type.StructField("sbi_kvk_secondary", ps_type.StringType(), True),
                        ps_type.StructField("sbi_kvk_tertiary", ps_type.StringType(), True),
                        ps_type.StructField("modify_date", ps_type.DateType(), True),
                        ]),
    # SECOND FILE << stage_sbi_predicted                        
    "SBI_PREDICTED"     : ps_type.StructType([
                        ps_type.StructField("company_id", ps_type.StringType(), True),
                        ps_type.StructField("sbi_predicted", ps_type.StringType(), True),
                        ps_type.StructField("sbi_predicted_version", ps_type.IntegerType(), True),
                        ps_type.StructField("sbi_predicted_probability", ps_type.DecimalType(7,6), True),
                        ps_type.StructField("modify_date", ps_type.DateType(), True),
                        ]),
    # FOURTH FILE << human_annotated_sbis.jsonl                        
    "SBI_MANUAL"        : ps_type.StructType([
                        ps_type.StructField("company_id", ps_type.StringType(), True),
                        ps_type.StructField("last_modified_date", ps_type.DateType(), True),
                        ps_type.StructField("manual_kvk_primary", ps_type.StringType(), True),
                        ps_type.StructField("manual_kvk_secondary", ps_type.StringType(), True),
                        ps_type.StructField("manual_kvk_tertiary", ps_type.StringType(), True),
                        ]),
}


# ----------------------------------------------------------------------------
# ods tables
# ----------------------------------------------------------------------------
schema_ods = {
    "ORGANIZATION"          : ps_type.StructType([
                            ps_type.StructField("company_id", ps_type.StringType(), True),
                            ps_type.StructField("active", ps_type.BooleanType(), True),
                            ps_type.StructField("create_date", ps_type.TimestampType(), True),
                            ps_type.StructField("modify_date", ps_type.TimestampType(), True),
                            ]),

    "BRANCH"                : ps_type.StructType([
                            ps_type.StructField("branch_id", ps_type.StringType(), True),
                            ps_type.StructField("company_id", ps_type.StringType(), True),
                            ps_type.StructField("active", ps_type.BooleanType(), True),
                            ps_type.StructField("create_date", ps_type.TimestampType(), True),
                            ps_type.StructField("modify_date", ps_type.TimestampType(), True),
                            ]),

    "SBI_ASSIGNED"          : ps_type.StructType([
                            ps_type.StructField("sbi_id", ps_type.StringType(), True),
                            ps_type.StructField("company_id", ps_type.StringType(), True),
                            ps_type.StructField("branch_id", ps_type.StringType(), True),
                            ps_type.StructField("sbi_source", ps_type.StringType(), True),
                            ps_type.StructField("sbi_level", ps_type.StringType(), True),
                            ps_type.StructField("active", ps_type.BooleanType(), True),
                            ps_type.StructField("create_date", ps_type.TimestampType(), True),
                            ps_type.StructField("modify_date", ps_type.TimestampType(), True),
                            ]),

    "SBI_DESCRIPTION"       : ps_type.StructType([
                            ps_type.StructField("sbi_id", ps_type.StringType(), True),
                            ps_type.StructField("language_id", ps_type.IntegerType(), True),
                            ps_type.StructField("sbi_short_description", ps_type.StringType(), True),
                            ps_type.StructField("active", ps_type.BooleanType(), True),
                            ps_type.StructField("create_date", ps_type.TimestampType(), True),
                            ps_type.StructField("modify_date", ps_type.TimestampType(), True),
                            ]),

    "LANGUAGE"              : ps_type.StructType([
                            ps_type.StructField("language_id", ps_type.IntegerType(), True),
                            ps_type.StructField("language_name", ps_type.StringType(), True),
                            ps_type.StructField("active", ps_type.BooleanType(), True),
                            ps_type.StructField("create_date", ps_type.TimestampType(), True),
                            ps_type.StructField("modify_date", ps_type.TimestampType(), True),
                            ]),
}