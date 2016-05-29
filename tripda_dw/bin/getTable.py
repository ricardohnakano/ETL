import argparse
import psycopg2 as pg
import pandas.io.sql as psql

def getTable(queryFile):
    try:
        conn = pg.connect("dbname='blabla' user='tripdaro' host='pgsql-ro.tripda.net' password='ohTh9eey'")
    except Exception as e:
        print "Unable to connect to the database"
        print e
    query =  open(queryFile).read()
    table = psql.read_sql(query, con=conn)
    return table

def saveTable(table, outputFilename, outputType):
    if(outputType == "xlsx"):
        table.to_excel(outputFilename, sheet_name='Sheet1', index=False)
    elif(outputType == "csv"):
        table.to_csv(outputFilename, index=False)

def main():
    
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("-i", "--input", required=True, action="store", dest="queryFile")
    parser.add_argument("-o", "--output", required=True, action="store", dest="outputFilename")
    parser.add_argument("-t", "--type", required=True, action="store", dest="outputType", choices=["csv", "xlsx"])
    args = parser.parse_args()
    
    table = getTable(args.queryFile)
    saveTable(table, args.outputFilename, args.outputType)

if __name__ == '__main__':
    main()