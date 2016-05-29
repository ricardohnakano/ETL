import csv
import pymysql
from datetime import datetime
import sys

DATA_DIR = "/home/voliveira/tripda-data-warehouse/data/"
#DATA_DIR = "/Users/vitor/Workspace/tripda-data-warehouse/data/"

def get_user_data(row):
    # birthdate
    if row[4] == '':
        row[4] = None
    # age
    if row[5] == '':
        row[5] = None
    # first_offer
    if row[32] == '':
        row[32] = None
    # last_offer
    if row[33] == '':
        row[33] = None  
    # first_driver_with_booking
    if row[34] == '':
        row[34] = None
    # last_driver_with_booking
    if row[35] == '':
        row[35] = None
    # first_driver
    if row[36] == '':
        row[36] = None
    # last_driver
    if row[37] == '':
        row[37] = None
    # first_booking
    if row[39] == '':
        row[39] = None
    # last_booking
    if row[40] == '':
        row[40] = None  
    # first_pax
    if row[41] == '':
        row[41] = None
      # last_pax
    if row[42] == '':
        row[42] = None
    return row

def main():
    #data_dir = "/home/voliveira/tripda-data-warehouse/data/"
    log = open(DATA_DIR + "log", "a")
    log.write(str(datetime.now()) + "-START-USERS-LOAD\n")
    csv_data = csv.reader(file(DATA_DIR+"users_transformed.csv"))
    conn = pymysql.connect(host='datateam-rds.tripda.net', port=3306, user='voliveira', passwd='eeCha?z0', db='data_team',  charset='utf8mb4', autocommit=False)
    #conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='', db='data_team',  charset='utf8mb4', autocommit=False)

    cursor = conn.cursor()
    #cursor.execute('LOAD DATA LOCAL INFILE \'/home/voliveira/tripda-data-warehouse/data/users_transformed.csv\' REPLACE INTO TABLE users FIELDS TERMINATED BY \',\' LINES TERMINATED BY \'\n\' IGNORE 1 LINES;')

    next(csv_data,None)
    cursor.execute('SET autocommit = 0')
    conn.commit()
    cursor.execute('START TRANSACTION')
    for row in csv_data:
        try: 
            user_data = get_user_data(row)
            cursor.execute('REPLACE INTO users(id,\
                                guid,\
                                first_name,\
                                last_name,\
                                birthdate,\
                                age,\
                                gender,\
                                created_at,\
                                country,\
                                locale,\
                                affiliate,\
                                phone_number,\
                                email,\
                                facebook_profile,\
                                pref_chat,\
                                pref_music,\
                                pref_smoking,\
                                pref_pets,\
                                pref_food,\
                                share_phone,\
                                share_email,\
                                share_facebook,\
                                share_nothing,\
                                facebook_friends,\
                                phone_verified,\
                                email_verified,\
                                badges,\
                                ratings,\
                                total_rating,\
                                have_comments,\
                                android,\
                                ios,\
                                first_offer,\
                                last_offer,\
                                first_driver_with_booking,\
                                last_driver_with_booking,\
                                first_driver,\
                                last_driver,\
                                total_offer,\
                                first_booking,\
                                last_booking,\
                                first_pax,\
                                last_pax,\
                                total_booking) VALUES (%s,%s,%s,\
                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', user_data)
        except Exception, e:
            print (str(datetime.now()) + " Error: "+str(e)+"\n At row: "+str(row)+"\n")
            log.write(str(datetime.now()) + "-ERROR-USERS-LOAD: " +str(e)+"\n At row: "+str(row)+ "\n")
            log.close()
            conn.rollback()
            cursor.close()
            conn.close()
            sys.exit(1)
    #close the connection to the database.
    conn.commit()
    cursor.close()
    conn.close()
    log.write(str(datetime.now()) + "-END-USERS-LOAD\n")
    log.close()
if __name__ == "__main__":
    main()
