import psycopg2 as pg
import pandas as pd
import pandas.io.sql as psql
from datetime import datetime

#SQL_EXTRACT = '/home/voliveira/tripda-data-warehouse/bin/extract-trips-table.sql'
SQL_EXTRACT = '/Users/vitor/Workspace/tripda-data-warehouse/bin/extract-trips-table.sql'
#DATA_DIR = "/home/voliveira/tripda-data-warehouse/data/"
DATA_DIR = "/Users/vitor/Workspace/tripda-data-warehouse/data/"

def transform(trips_table):
	
    #trips_table.promo_code.fillna(0,inplace=True)
	#trips_table.description_code.fillna(0,inplace=True)

	trips_table.seats_booked.fillna(0,inplace=True)
	trips_table.seats_approved.fillna(0,inplace=True)
	trips_table.seats_canceled.fillna(0,inplace=True)
	trips_table.seats_rejected.fillna(0,inplace=True)

	trips_table.driver_message.fillna(0,inplace=True)
	trips_table.pax_message.fillna(0,inplace=True)

def extract():
    try:
        conn = pg.connect("dbname='blabla' user='tripdaro' host='pgsql-ro.tripda.net' password='ohTh9eey'")
    except Exception as e:
        print "Unable to connect to the database"
        print e    
    trips_query = open(SQL_EXTRACT).read()

    cursor = conn.cursor()

    outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(trips_query)

    with open(DATA_DIR+"trips_extracted.csv", 'w') as f:
        cursor.copy_expert(outputquery, f)

    #trips_table =  pd.read_csv(DATA_DIR+"trips_extracted.csv", sep=',', low_memory=False, error_bad_lines=False, index_col=False, dtype='unicode')

    conn.close()
    #return trips_table

def main():
    #data_dir = "C:\Users\Bruno\Documents\Tripda\DW\\trips"
    log = open(DATA_DIR + "log", "a")
    log.write(str(datetime.now()) + "-START-TRIPS-EXTRACT-TRANSFORM\n")
    log.write(str(datetime.now()) +  "-START-TRIPS-EXTRACT\n")    
    extract()
    #trips_table = extract()
    log.write(str(datetime.now()) +  "-END-TRIPS-EXTRACT\n")
    log.write(str(datetime.now()) +  "-START-TRIPS-TRANSFORM\n")
    #transform(trips_table)
    log.write(str(datetime.now()) +  "-END-TRIPS-TRANSFORM\n")
    log.write(str(datetime.now()) +  "-START-TRIPS-SAVECSV\n")
    #trips_table.to_csv(DATA_DIR+"trips_transformed.csv", index=False)
    log.write(str(datetime.now()) +  "-END-TRIPS-SAVECSV\n")
    log.write(str(datetime.now()) + "-END-TRIPS-EXTRACT-TRANSFORM\n")
    log.close()
    

if __name__ == "__main__":
    main()