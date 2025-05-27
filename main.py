import sys, getopt


import src.create_tables        as setup
import src.load_flat_files      as load_flat_files
import src.load_to_staging      as load_to_staging
import src.load_to_ods          as load_to_ods

def main(argv) -> None:
    try:
        options, args = getopt(argv, 'arso')
    except getopt.GetoptError:
        print ('main.py -a')
        sys.exit(2)


    # set up
    #   -- create tables
    setup.create_tables()

    # load raw
    #   -- load files to raw tables
    load_flat_files.procces_raw_files()

    # load stage
    #   -- move from raw to stage
    load_to_staging.procces_staging()


    # load ods
    #   -- movee from stage to ODS
    load_to_ods.procces_ods()


if __name__ == '__main__':
    main(sys.argv[1:])