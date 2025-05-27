READ_ME
KVK project
Great you are reading the documentation. Although there really isn’t much to say about implementation.  
Use the requirements.txt to install the necessary packages. 

The project shows how to use pyspark to load .csv and .json files.  I used KVK data as sample implementation.
The project will load the data from raw >> stage >> ods tables, i.e. following a medallion structure of bronze >> silver >> gold.

The project will load all of the data files are stored in relative directory from the project.  
In the Data folder you will need to include the (3) .csv files and (1) .json file. The parquet files will be created in relative folder from the project.

Start the project by running the main.py.  There are no arguments / switches for the file.  
$ python main.py

SBI
    data
    spark-warehouse
main.py

The main.py will first create the tables, and then start processing the data files. 

The components directory contains all of the constants needed for referencing the data files and tables.

The processing directory contains all of the files for moving the data from raw >> stage >> ods tables.

There is nothing in the tests directory. 
