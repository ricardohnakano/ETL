import psycopg2 as pg
import pandas as pd
import pandas.io.sql as psql
from datetime import datetime

SQL_EXTRACT = '/home/voliveira/tripda-data-warehouse/bin/extract-bookings-table.sql'
#SQL_EXTRACT = '/Users/vitor/Workspace/tripda-data-warehouse/bin/extract-bookings-table.sql'
DATA_DIR = "/home/voliveira/tripda-data-warehouse/data/"
#DATA_DIR = "/Users/vitor/Workspace/tripda-data-warehouse/data/"

def extract():
    try:
        conn = pg.connect("dbname='blabla' user='tripdaro' host='pgsql-ro.tripda.net' password='ohTh9eey'")
    except Exception as e:
        print "Unable to connect to the database"
        print e    
    bookings_query = open(SQL_EXTRACT).read()

    cursor = conn.cursor()

    outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(bookings_query)

    with open(DATA_DIR+"bookings_extracted.csv", 'w') as f:
        cursor.copy_expert(outputquery, f)

    bookings_table =  pd.read_csv(DATA_DIR+"bookings_extracted.csv", sep=',', error_bad_lines=False, index_col=False, dtype='unicode')

    conn.close()
    return bookings_table

def main():
    #data_dir = "/home/voliveira/tripda-data-warehouse/data/"
    log = open(DATA_DIR + "log", "a")
    log.write(str(datetime.now()) + "-START-BOOKINGS-EXTRACT-TRANSFORM\n")
    log.write(str(datetime.now()) +  "-START-BOOKINGS-EXTRACT\n")
    bookings_table = extract()
    log.write(str(datetime.now()) +  "-END-BOOKINGS-EXTRACT\n")
    log.write(str(datetime.now()) +  "-START-BOOKINGS-SAVECSV\n")
    bookings_table.to_csv(DATA_DIR+"bookings_transformed.csv", index=False)
    log.write(str(datetime.now()) +  "-END-BOOKINGS-SAVECSV\n")
    log.write(str(datetime.now()) + "-END-BOOKINGS-EXTRACT-TRANSFORM\n")
    log.close()

if __name__ == "__main__":
    main()