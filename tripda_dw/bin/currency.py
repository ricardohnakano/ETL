import time
from datetime import datetime
from datetime import timedelta
import os
import numpy as np
import psycopg2 as pg
import pandas as pd
import pandas.io.sql as psql
import urllib2
import json
import pymysql
import csv
import os.path


#SQL_EXTRACT = '/home/voliveira/tripda-data-warehouse/bin/extract-bookings-table.sql'
#SQL_EXTRACT = '/Users/vitor/Workspace/tripda-data-warehouse/bin/extract-bookings-table.sql'
DATA_DIR = "/home/voliveira/tripda-data-warehouse/data/"
#DATA_DIR = "/home/rnakano/DW/currency/"
#DATA_DIR = "/Users/vitor/Workspace/tripda-data-warehouse/data/"

def currencyConverter(currency_from,currency_to,currency_input):
    yql_base_url = "https://query.yahooapis.com/v1/public/yql"
    yql_query = 'select%20*%20from%20yahoo.finance.xchange%20where%20pair%20in%20("'+currency_from+currency_to+'")'
    yql_query_url = yql_base_url + "?q=" + yql_query + "&format=json&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys"
    try:
        yql_response = urllib2.urlopen(yql_query_url)
        try:
            yql_json = json.loads(yql_response.read())
            currency_output = currency_input * float(yql_json['query']['results']['rate']['Rate'])
            return currency_output
        except (ValueError, KeyError, TypeError):
            return "JSON format error"
    except IOError, e:
        if hasattr(e, 'code'):
            return e.code
        elif hasattr(e, 'reason'):
            return e.reason


def currency_routine(currency_from,currency_input,index):
    i = datetime.now()
    
    bcsv_one = False
    bcsv_two = False
    bcsv_three = False
    conversion = 'error'
    default_currency = ['0.319','0.00037','0.058', '0.24','0.0014','0.1','0.018','0.62','0.026','0.81','0.013','0.033','0.008']

    i = i -timedelta(days=1)
    ReferenceDate = i.strftime('%Y-%m-%d')
    if os.path.isfile(DATA_DIR + "currency/currency_" + ReferenceDate + ".csv") == True:
        currency_one = pd.read_csv(DATA_DIR + "currency/currency_" + ReferenceDate + ".csv")
        dict_one = currency_one['rate'].to_dict()
        bcsv_one = True

    i = i -timedelta(days=1)
    ReferenceDate = i.strftime('%Y-%m-%d')
    if os.path.isfile(DATA_DIR + "currency/currency_" + ReferenceDate + ".csv") == True:
        currency_two = pd.read_csv(DATA_DIR + "currency/currency_" + ReferenceDate + ".csv")
        dict_two = currency_two['rate'].to_dict()
        bcsv_two = True

    i = i -timedelta(days=1)
    ReferenceDate = i.strftime('%Y-%m-%d')
    if os.path.isfile(DATA_DIR + "currency/currency_" + ReferenceDate + ".csv") == True:
        currency_three = pd.read_csv(DATA_DIR + "currency/currency_" + ReferenceDate + ".csv")
        dict_three = currency_three['rate'].to_dict()
        bcsv_three = True

    bStop = False
    for iCounter in range(0,2):
        if bStop == False:
            if type(currencyConverter(currency_from,'EUR',1)) == type(float()):
                conversion = currencyConverter(currency_from,'EUR',1)
                bStop = True

    if type(conversion) != type(float()):
        if bcsv_one == True:
            conversion = float(dict_one[index])

    if type(conversion) != type(float()):
        if bcsv_two == True:
            conversion = float(dict_two[index])

    if type(conversion) != type(float()):
        if bcsv_three == True:
            conversion = float(dict_three[index])

    if type(conversion) != type(float()) or conversion == 0.999999:
        conversion = float(default_currency[index])
        #print 7    
            
    return conversion


def currency():
    #pd.set_option('display.max_columns',None)
    today = datetime.now()
    CurrentDate = today.strftime('%Y-%m-%d')

    AbrevCountryList = ['BRL','COP','MXN','MYR','CLP','ARS','PHP','SGD','TWD','USD','INR','UYU','PKR']
    CountryList = ['Brasil','Colombia','Mexico','Malaysia','Chile','Argentina','Philippines','Singapore','Taiwan','United States','India','Uruguay','Pakistan']

    currency_table = pd.read_csv(DATA_DIR + 'currency/currency.csv')

    length = len(currency_table)
    count = 0
    for i in AbrevCountryList:
        NewRow = []
        NewRow.append(CurrentDate + CountryList[count])
        NewRow.append(CurrentDate)
        NewRow.append(CountryList[count])
        currency_counrty = currency_routine(i,1,count)
        NewRow.append(currency_counrty)
        time.sleep(1)
        currency_table.loc[count + length] = np.array(NewRow)
        count = count + 1
    return currency_table


def load(CurrentDate):
	csv_data = csv.reader(file(DATA_DIR + "currency/currency_"+ CurrentDate + ".csv"))
	conn = pymysql.connect(host='datateam-rds.tripda.net', port=3306, user='voliveira', passwd='eeCha?z0', db='data_team',  charset='utf8mb4')
	cursor = conn.cursor()

	next(csv_data,None)
	for row in csv_data:
		cursor.execute('REPLACE INTO currency(id,date, country,rate)VALUES(%s,%s,%s,%s)', row)
	    
	conn.commit()
	cursor.close()


def main():
    log = open(DATA_DIR + "currency_log", "a")
    log.write(str(datetime.now()) + "++++++++++START - CURRENCY UPDATE ++++++++\n")

    log.write(str(datetime.now()) +  "-START-CURRENCY-GENERATING DATA\n")
    currency_table = currency()
    log.write(str(datetime.now()) +  "- END -  CURRENCY-GENERATING DATA\n")

    log.write(str(datetime.now()) +  "-START-CURRENCY-SAVECSV\n")
    i = datetime.now()
    CurrentDate = i.strftime('%Y-%m-%d')
    currency_table.to_csv(DATA_DIR + "currency/currency_" + CurrentDate + ".csv", index=False)
    log.write(str(datetime.now()) +  "- END -CURRENCY-SAVECSV\n")

    log.write(str(datetime.now()) +  "-START-CURRENCY-LOAD\n")
    load(CurrentDate)
    log.write(str(datetime.now()) +  "- END -CURRENCY-LOAD\n")

    log.write(str(datetime.now()) + "----------END-CURRENCY UPDATE ---------\n")
    log.close()

if __name__ == "__main__":
    main()