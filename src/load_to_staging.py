import processing.stage_sbi_defintion   as stg_def
import processing.stage_sbi_kvk         as stg_kvk
import processing.stage_sbi_predicted   as stg_pred
import processing.stage_sbi_manual      as stg_man

def procces_staging() -> bool:
    """load from the raw tables and land them into the staging tables"""
    stg_def.process_sbi_defintion()
    stg_kvk.process_sbi_kvk()
    stg_pred.process_sbi_predicted()
    stg_man.process_sbi_manual()

    return(True)

