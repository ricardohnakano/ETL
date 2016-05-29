import psycopg2 as pg
import pandas as pd
import pandas.io.sql as psql
from datetime import datetime

SQL_EXTRACT = '/home/voliveira/tripda-data-warehouse/bin/extract-users-table.sql'
#SQL_EXTRACT = '/Users/vitor/Workspace/tripda-data-warehouse/bin/extract-users-table.sql'
DATA_DIR = "/home/voliveira/tripda-data-warehouse/data/"
#DATA_DIR = "/Users/vitor/Workspace/tripda-data-warehouse/data/"


def transform(users_table):

    users_table.pref_chat.fillna(0,inplace=True)
    users_table.pref_music.fillna(0,inplace=True)
    users_table.pref_smoking.fillna(0,inplace=True)
    users_table.pref_pets.fillna(0,inplace=True)
    users_table.pref_food.fillna(0,inplace=True)

    users_table.share_phone.fillna(0,inplace=True)
    users_table.share_email.fillna(0,inplace=True)
    users_table.share_facebook.fillna(0,inplace=True)
    users_table.share_nothing.fillna(0,inplace=True)

    users_table.facebook_friends.fillna(0,inplace=True) 

    users_table.phone_verified.fillna(0,inplace=True)
    users_table.email_verified.fillna(0,inplace=True)

    users_table.ratings.fillna(0,inplace=True)
    users_table.total_rating.fillna(0,inplace=True)
    users_table.have_comments.fillna(0,inplace=True)
    users_table.android.fillna(0,inplace=True)
    users_table.ios.fillna(0,inplace=True)

    users_table.total_offer.fillna(0,inplace=True)
    users_table.total_booking.fillna(0,inplace=True)


def extract():
    #try:
    #    conn = pg.connect("dbname='blabla' user='tripdaro' host='pgsql-ro.tripda.net' password='ohTh9eey'")
    #except Exception as e:
    #    print "Unable to connect to the database"
    #    print e
    #users_query =  open('extract-users-table.sql').read()
    #users_table = psql.read_sql(users_query, con=conn)
    #conn.close()
    #return users_table

    try:
        conn = pg.connect("dbname='blabla' user='tripdaro' host='pgsql-ro.tripda.net' password='ohTh9eey'")
    except Exception as e:
        print "Unable to connect to the database"
        print e
    users_query =  open(SQL_EXTRACT).read()

    cursor = conn.cursor()

    outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(users_query)

    with open(DATA_DIR+"users_extracted.csv", 'w') as f:
        cursor.copy_expert(outputquery, f)
    
    low_memory=False
    users_table =  pd.read_csv(DATA_DIR+"users_extracted.csv", sep=',', error_bad_lines=False, index_col=False, dtype='unicode')

    conn.close()
    return users_table

def main():
    #data_dir = "/home/voliveira/tripda-data-warehouse/data/"
    log = open(DATA_DIR + "log", "a")
    log.write(str(datetime.now()) + "-START-USERS-EXTRACT-TRANSFORM\n")
    log.write(str(datetime.now()) +  "-START-USERS-EXTRACT\n")
    users_table = extract()
    log.write(str(datetime.now()) +  "-END-USERS-EXTRACT\n")
    log.write(str(datetime.now()) +  "-START-USERS-TRANSFORM\n")
    transform(users_table)
    log.write(str(datetime.now()) +  "-END-USERS-TRANSFORM\n")
    log.write(str(datetime.now()) +  "-START-USERS-SAVECSV\n")
    users_table.to_csv(DATA_DIR+"users_transformed.csv", index=False)
    log.write(str(datetime.now()) +  "-END-USERS-SAVECSV\n")
    log.write(str(datetime.now()) + "-END-USERS-EXTRACT-TRANSFORM\n")
    log.close()
    
if __name__ == "__main__":
    main()
