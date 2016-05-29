import time
import datetime
import os
import csv
import pymysql
from datetime import datetime

#DATA_DIR = "/home/voliveira/tripda-data-warehouse/"
DATA_DIR = "/home/voliveira/tripda-data-warehouse/data/"
#DATA_DIR = "/Users/vitor/Workspace/tripda-data-warehouse/data/"


def load():
    csv_data = csv.reader(file(DATA_DIR+"cube_transformed.csv"))
    conn = pymysql.connect(host='datateam-rds.tripda.net', port=3306, user='voliveira', passwd='eeCha?z0', db='data_team',  charset='utf8')
    cursor = conn.cursor()

    next(csv_data,None)
    for row in csv_data:
        cursor.execute('REPLACE INTO cube(id,\
                                           reference_date,\
                                           country,\
                                           new_users,\
                                           new_driver,\
                                           new_passenger,\
                                           new_trip_offered,\
                                           ask,\
                                           trip_offered,\
                                           new_booking,\
                                           trip_realized,\
                                           pax_transported,\
                                           seats_price,\
                                           seats_distance,\
                                           trip_cancelation,\
                                           booking_cancelation,\
                                           booking_rejection)VALUES(%s,%s,%s,%s,%s\
                                           ,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', row)
        
    conn.commit()
    cursor.close()



def main():
    #data_dir = "C:\Users\Bruno\Documents\Tripda\DW\\trips"
    log = open(DATA_DIR + "log", "a")
    log.write(str(datetime.now()) + "+++++++++ START-CUBE-LAOD ++++++++++\n")

    log.write(str(datetime.now()) +  "-START-CUBE-LAOD\n")
    load()
    log.write(str(datetime.now()) +  "- END -CUBE-LAOD\n")

    log.write(str(datetime.now()) +  "---------- END-CUBE-LAOD ------------\n")
    log.close()

if __name__ == "__main__":
    main()