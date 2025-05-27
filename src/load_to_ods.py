import processing.ods_branch            as ods_bra
import processing.ods_language          as ods_lan
import processing.ods_organization      as ods_org
import processing.ods_sbi_assign        as ods_sbi
import processing.ods_sbi_description   as ods_des

def procces_ods() -> bool:
    """load from the staging tables and land them into the ods tables"""
    ods_lan.process_ODS_language()
    ods_org.process_ods_organization()
    ods_bra.process_ods_branch()
    ods_des.process_ods_sbi_description()
    
    ods_sbi.process_ods_branch_kvk()
    ods_sbi.process_ods_branch_predicted()
    ods_sbi.process_ods_sbi_manual()

    return(True)

