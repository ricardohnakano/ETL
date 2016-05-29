import csv
import pymysql
from datetime import datetime
import sys

DATA_DIR = "/home/voliveira/tripda-data-warehouse/data/"
#DATA_DIR = "/Users/vitor/Workspace/tripda-data-warehouse/data/"

def get_trip_data(row):
    # promocode
    if row[31] == '':
        row[31] = None
    # description code
    if row[32] == '':
        row[32] = None
    return row

def main():
    log = open(DATA_DIR + "log", "a")
    log.write(str(datetime.now()) + "-START-TRIPS-LOAD\n")
    #csv_data = csv.reader(file(DATA_DIR+"trips_transformed.csv"))
    csv_data = csv.reader(file(DATA_DIR+"trips_extracted.csv"))

    
    conn = pymysql.connect(host='datateam-rds.tripda.net', port=3306, user='voliveira', passwd='eeCha?z0', db='data_team',  charset='utf8mb4', autocommit=False)
    #conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='', db='dw',  charset='utf8mb4')
    cursor = conn.cursor()
    #cursor.execute('LOAD DATA INFILE \'/home/voliveira/tripda-data-warehouse/data/trips_transformed.csv\' REPLACE INTO TABLE trips FIELDS TERMINATED BY \',\' LINES TERMINATED BY \'\n\' IGNORE 1 LINES;')

    next(csv_data,None)
    cursor.execute('SET autocommit = 0')
    conn.commit()
    cursor.execute('START TRANSACTION')
    for row in csv_data:
        try:
            trips_data = get_trip_data(row)
            cursor.execute('REPLACE INTO trips(id,\
								tc_id,\
								trip_id,\
								main_leg,\
								trip_link,\
								driver_id,\
								trip_created_at,\
								trip_departure_datetime,\
                                departure_address,\
                                destination_address,\
								departure_city,\
								destination_city,\
								departure_state,\
								destination_state,\
								departure_country,\
								destination_country,\
								country,\
								seats_offered,\
								trip_distance,\
								suggested_price,\
								price,\
								trip_canceled,\
								auto_accept,\
								trip_comment,\
								driver_message,\
								pax_message,\
								recurrent,\
								ladies_only,\
								is_return,\
								seats_booked,\
								seats_approved,\
								seats_canceled,\
								seats_rejected,\
								promo_code,\
								description_code) VALUES (%s,%s,%s,\
                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', trips_data)
        except Exception, e:
            #print "\n",e
            #print "\n",row,"\n"
            print (str(datetime.now()) + " Error: "+str(e)+"\n At row: "+str(row)+"\n")
            log.write(str(datetime.now()) + "-ERROR-TRIPS-LOAD: " +str(e)+"\n At row: "+str(row)+ "\n")
            log.close()
            conn.rollback()
            cursor.close()
            conn.close()
            sys.exit(1)
    #close the connection to the database.
    conn.commit()
    cursor.close()
    conn.close()
    log.write(str(datetime.now()) + "-END-TRIPS-LOAD\n")
    log.close()
if __name__ == "__main__":
    main()
