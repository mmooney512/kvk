READ_ME
SBI project
Great you are reading the documentation. Although there really isn’t much to say about implementation.  
Use the requirements.txt to install the necessary packages. 
All the data files are stored in relative directory from the project.  
In the Data folder you will need to include the (4) files that were sent to me. The parquet files will be created in relative folder from the project.

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
