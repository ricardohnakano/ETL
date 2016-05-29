import csv
import pymysql
from datetime import datetime
import sys


DATA_DIR = "/home/voliveira/tripda-data-warehouse/data/"
#DATA_DIR = "/Users/vitor/Workspace/tripda-data-warehouse/data/"

def main():
    #data_dir = "/home/voliveira/tripda-data-warehouse/data/"
    log = open(DATA_DIR + "log", "a")
    log.write(str(datetime.now()) + "-START-BOOKINGS-LOAD\n")
    csv_data = csv.reader(file(DATA_DIR+"bookings_transformed.csv"))
    
    conn = pymysql.connect(host='datateam-rds.tripda.net', port=3306, user='voliveira', passwd='eeCha?z0', db='data_team',  charset='utf8mb4', autocommit=False)
    #conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='', db='dw1',  charset='utf8')
    cursor = conn.cursor()
    next(csv_data,None)
    cursor.execute('SET autocommit = 0')
    conn.commit()
    cursor.execute('START TRANSACTION')
    for row in csv_data:
        bookings_data = row        
        try:
            cursor.execute('REPLACE INTO bookings(id,\
                            related_trip_id,\
                            trip_id,\
                            trip_created_at,\
                            trip_departure_datetime,\
                            departure_city,\
                            destination_city,\
                            departure_state,\
                            destination_state,\
                            departure_country,\
                            destination_country,\
                            country,\
                            trip_distance,\
                            trip_canceled,\
                            driver_id,\
                            passenger_id,\
                            seats_booked,\
                            booking_created_at,\
                            is_approved,\
                            booking_canceled,\
                            booking_rejected,\
                            price,\
                            ratings) VALUES (%s,%s,%s,\
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', bookings_data)
            
        except Exception, e:
            print (str(datetime.now()) + " Error: "+str(e)+"\n At row: "+str(row)+"\n")
            log.write(str(datetime.now()) + "-ERROR-BOOKINGS-LOAD: " +str(e)+"\n At row: "+str(row)+ "\n")
            log.close()
            conn.rollback()
            cursor.close()
            conn.close()
            sys.exit(1)
    #close the connection to the database.
    conn.commit()
    cursor.close()
    conn.close()
    log.write(str(datetime.now()) + "-END-BOOKINGS-LOAD\n")
    log.close()

if __name__ == "__main__":
    main()