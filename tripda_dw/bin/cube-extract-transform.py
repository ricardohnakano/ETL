import time
import datetime
import os
import psycopg2 as pg
import pandas as pd
import pandas.io.sql as psql
import pymysql
import csv
from datetime import datetime

#DATA_DIR = "/home/voliveira/tripda-data-warehouse/"
SQL_EXTRACT = '/home/voliveira/tripda-data-warehouse/bin/extract-cube-table.sql'
DATA_DIR = "/home/voliveira/tripda-data-warehouse/data/"
#DATA_DIR = "/Users/vitor/Workspace/tripda-data-warehouse/data/"

def extract():
    try:
        conn = pymysql.connect(host='datateam-rds.tripda.net', port=3306, user='voliveira', passwd='eeCha?z0', db='data_team',  charset='utf8')
    except Exception as e:
        print "Unable to connect to the database"
        print e

    cube_query = open(SQL_EXTRACT).read()

    cursor = conn.cursor()

    cube_table = psql.read_sql(cube_query, con=conn)

    conn.close()
    return cube_table


def transform(cube_table):
    cube_table.new_users.fillna(0,inplace=True)
    cube_table.new_driver.fillna(0,inplace=True)
    cube_table.new_passenger.fillna(0,inplace=True)
    cube_table.new_trip_offered.fillna(0,inplace=True)
    cube_table.ask.fillna(0,inplace=True)
    cube_table.trip_offered.fillna(0,inplace=True)
    cube_table.new_booking.fillna(0,inplace=True)
    cube_table.trip_realized.fillna(0,inplace=True)
    cube_table.pax_transported.fillna(0,inplace=True)
    cube_table.seats_price.fillna(0,inplace=True)
    cube_table.seats_distance.fillna(0,inplace=True)
    cube_table.trip_cancelation.fillna(0,inplace=True)
    cube_table.booking_cancelation.fillna(0,inplace=True)
    cube_table.booking_rejection.fillna(0,inplace=True)


def main():
    #data_dir = "C:\Users\Bruno\Documents\Tripda\DW\\trips"
    log = open(DATA_DIR + "log", "a")
    log.write(str(datetime.now()) + "+++++++++ START-CUBE-EXTRACT-TRANSFORM ++++++++++\n")

    log.write(str(datetime.now()) +  "-START-CUBE-EXTRACT\n")
    cube_table = extract()
    log.write(str(datetime.now()) +  "- END -CUBE-EXTRACT\n")

    log.write(str(datetime.now()) +  "-START-CUBE-TRANSFORM\n")
    transform(cube_table)
    log.write(str(datetime.now()) +  "- END -CUBE-TRANSFORM\n")

    log.write(str(datetime.now()) +  "-START-CUBE-SAVECSV\n")
    cube_table.to_csv(DATA_DIR+"cube_transformed.csv", index=False)
    log.write(str(datetime.now()) +  "- END -CUBE-SAVECSV\n")

    log.write(str(datetime.now()) +  "---------- END-CUBE-EXTRACT-TRANSFORM ------------\n")
    log.close()
    

if __name__ == "__main__":
    main()