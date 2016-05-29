import psycopg2 as pg
import pandas as pd
import pandas.io.sql as psql
import sys,os,re
from datetime import date, timedelta

error = False

if len(sys.argv) == 1:
	#run code without arguments(python report_generator.py)
	error = True
elif len(sys.argv) == 2:
	argument1 = sys.argv[1]
	argument2 = os.getcwd() + '/reports'
	argument3 = ''
elif len(sys.argv) == 3:
	argument1 = sys.argv[1]
	argument2 = sys.argv[2]
	argument3 = os.getcwd() + '/reports'
elif len(sys.argv) == 4:
	argument1 = sys.argv[1]
	argument2 = sys.argv[2]
	argument3 = sys.argv[3]

def help():
	print '\n================\nREPORT GENERATOR\n================'
	print '\nHOW TO USE:\n python report_generator.py fisrt_argument second_argument third_argument'
	print '\nWHAT DOES THIS CODE DO:\n connect to the product database, run a query and export to csv'
	print '\nFIRST ARGUMENT POSSIBILITIES:\n -daily\n -weekly\n -monthly\n -none'
	print '\nIF YOU CHOOSE: daily, weekly or monthly:\n -it will run the repective report\n -the sql query need to be on the same directory of this python code \n -if the name of the sql query changes on git, this code need to change too\n -(optional) the second argument can be the destination folder [default - tripda-data-warehouse/report queries/reports]'
	print '\nIF YOU CHOOSE: none\n -the second argument is the sql query\n -(optional) the third argument can be the destination folder [default - tripda-data-warehouse/report queries/reports]\n -the query need to have: country, report_reference, reference_number, report_year and all columns of the daily sql'
	print '\nREPORT_REFERENCE AND REFERENCE_NUMBER:\n -report_reference is the period that you whant to analyse \n  (eg.: weekly = "26.Dec - 01.Jan" or "19.Dec - 25.Dec")\n -reference_number is the number correspondent to the report_reference\n  (eg.: "26.Dec - 01.Jan" = 52)\n'

def extract(query):
    try:
		conn = pg.connect("dbname='blabla' user='tripdaro' host='pgsql-ro.tripda.net' password='ohTh9eey'")
    except Exception as e:
        print e
        print "Unable to connect to the database"

    daily_query = open(query,'r').read()

    cursor = conn.cursor()

    db_table = psql.read_sql(daily_query, con=conn)  


    conn.close()
    return db_table

def transform(table):
	df = table.fillna(0)
	df.loc[df.country == "Argentina", "country"] = "Argentina + Uruguay"
	df.loc[df.country == "Uruguay", "country"] = "Argentina + Uruguay"
	df.loc[df.country == "Malaysia", "country"] = "Malaysia + Singapore"
	df.loc[df.country == "Singapore", "country"] = "Malaysia + Singapore"

	combined_countries = df.groupby(["country","report_reference","reference_number","report_year"],as_index=False, sort=False).sum()
	combined_total = df.groupby(["report_reference","reference_number","report_year"],as_index=False, sort=False).sum()

	combined_total["country"] = " Total"

	report = combined_total.append(combined_countries,True)

	report["avg_pax/trip"] = report["pax_transported"]/report["trip_realized"]
	report["Realize_Trip_Yield"] = 100*(report["trip_realized"]/report["trip_offered"])
	report["Avg_Realized_Km"] = report["seats_km"]/report["pax_transported"]
	report["Driver_Cancel_Rate"] = 100*report["driver_cancelation"]/(report["pax_transported"]+report["pax_cancelation"]+report["driver_rejection"]+ report["driver_cancelation"])
	report["Pax_Cancel_Rate"] = 100*report["pax_cancelation"]/(report["pax_transported"]+report["pax_cancelation"]+report["driver_rejection"]+ report["driver_cancelation"])

	pd.set_option('precision', 2)
	report = report.fillna(0)

	report.sort(['country','report_year','reference_number'], ascending=[1,0,0], inplace=True)

	columns = ['country','report_year','report_reference','new_users','new_drivers','new_passangers','new_trip_offered','new_bookings','ask',\
				'trip_realized', 'unique_trip_realized','pax_transported','unique_pax_driver','avg_pax/trip','Realize_Trip_Yield','Avg_Realized_Km','Driver_Cancel_Rate','Pax_Cancel_Rate']
	report = report[columns]
	
	return report

def export(final_table, directory):
	final_table.to_csv(directory + "/report_generator.csv", index=False)

def main():
	global error
	if error == False:
		if argument1 == 'help':
			help()
		elif argument1 == 'daily':
			dataframe = extract('Daily_Fix.sql')
			transformed_df = transform(dataframe)
			export(transformed_df, argument2)
		elif argument1 == 'weekly':
			dataframe = extract('Weekly_Fix.sql')
			transformed_df = transform(dataframe)
			export(transformed_df, argument2)
		elif argument1 == 'monthly':
			dataframe = extract('Monthly_Fix.sql')
			transformed_df = transform(dataframe)
			export(transformed_df, argument2)
		elif argument1 == 'none':
			if os.path.isfile(argument2) == True:
				dataframe =  extract(argument2)
				transformed_df = transform(dataframe)
				export(transformed_df, argument3)
			else:
				print '\n\nUnknown query ' + argument2 + '. Check if the sql file is in the same folder of the python code or if you type the correct directory for it.\n\n'
				error = True
		else:
			print '\n\nInvalid arguments. Try: python report_generator.py help\n\n'
			error = True
	else:
		print '\n\nScript without arguments. Try: python report_generator.py help\n\n'
		error = True


if __name__ == "__main__":
   main()

