import psycopg2 as pg
import pandas as pd
import pandas.io.sql as psql
import sys,os,re
from datetime import datetime
import pymysql

from time import sleep
from smtplib import SMTP
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEImage import MIMEImage

import pdfkit
import wkhtmltopdf
from datetime import date, timedelta

SQL_EXTRACT = '/home/voliveira/tripda-data-warehouse/bin/extract-bookings-table.sql'
#SQL_EXTRACT = '/Users/vitor/Workspace/tripda-data-warehouse/bin/extract-bookings-table.sql'
DATA_DIR = "/home/rnakano/Daily/"
#DATA_DIR = "/Users/vitor/Workspace/tripda-data-warehouse/data/"

#import os
#os.chdir("C:\Users\\ricardo.nakano\Dropbox\BI\Performance Reports\Daily")

def extract():
    try:
        conn = pymysql.connect(host='datateam-rds.tripda.net', port=3306, user='voliveira', passwd='eeCha?z0', db='data_team',  charset='utf8')
    except Exception as e:
        print "Unable to connect to the database"
        print e

    daily_query = open(DATA_DIR + 'daily_query_DW.sql','r').read()

    cursor = conn.cursor()

    db_table = psql.read_sql(daily_query, con=conn)  


    conn.close()
    return db_table


def transform(db_table):
    db_table = db_table.convert_objects(convert_numeric=True)
    db_table['country'] = db_table['country'].astype(str)
    db_table['report_date'] = db_table['reference_date'].astype(datetime)
    db_table.drop('reference_date', axis=1, inplace=True)

    db_table = db_table.fillna(0)
    total = db_table.groupby(["report_date"],as_index=False, sort=False).sum()
    total["id"] = "Total"
    total["country"] = "Total"

    daily_report = total.append(db_table,True)

    daily_report["avg_pax/trip"] = daily_report["pax_transported"]/daily_report["trip_realized"]
    daily_report["Realize_Trip_Yield"] = 100*(daily_report["trip_realized"]/daily_report["trip_offered"])
    daily_report["Avg_Realized_Km"] = daily_report["seats_distance"]/daily_report["pax_transported"]
    daily_report["Driver_Cancel_Rate"] = 100*daily_report["trip_cancelation"]/(daily_report["pax_transported"])
    daily_report["Pax_Cancel_Rate"] = 100*daily_report["booking_cancelation"]/(daily_report["pax_transported"])

    pd.set_option('precision', 2)
    daily_report = daily_report.fillna(0)

    daily_report.drop('id', axis=1, inplace=True)

    from datetime import date, timedelta
    yesterday = date.today() - timedelta(1)
    df_sorting = daily_report.loc[daily_report.report_date == yesterday, ["country", "pax_transported"]]
    mapped_values = df_sorting.set_index('country')['pax_transported'].to_dict()
    daily_report['sort'] = daily_report['country'].map(lambda x: mapped_values[x])
    daily_report.sort(['sort', 'country', 'report_date'], ascending=[0,1,0], inplace=True)
    
    return daily_report


def html (daily_report, index):
    import pandas
    import numpy as np

    arr = daily_report[['ask','booking_cancelation','booking_rejection','country','new_booking',\
                        'new_driver','new_passenger','new_trip_offered','new_users','pax_transported',\
                        'report_date','seats_distance','seats_price','trip_cancelation','trip_offered',\
                        'trip_realized','avg_pax/trip','Realize_Trip_Yield','Avg_Realized_Km',\
                        'Driver_Cancel_Rate','Pax_Cancel_Rate','sort']].to_dict()


    rowNumber = max(daily_report.index)
    rows = daily_report.index

    headerQuery = ['country','report_date','new_users','new_driver','new_passenger','new_trip_offered','new_booking', \
                   'ask','trip_realized','pax_transported','avg_pax/trip','Realize_Trip_Yield', \
                   'Avg_Realized_Km','Driver_Cancel_Rate','Pax_Cancel_Rate']


    rate_format = lambda x: str('%.1f' % x) + '%'
    average_format = lambda x: str('%.1f' % x)
    default_format = lambda x: str('%.0f') % x

    index_rate_format = ['Realize_Trip_Yield','Pax_Cancel_Rate','Driver_Cancel_Rate']
    index_average_format = ['avg_pax/trip','Avg_Realized_Km']
    index_default_format = ['new_users','new_driver','new_passenger','new_trip_offered','ask','new_bookings','trip_realized','pax_transported']

    ######################## HTML ##########################################

    ### TEXTO INICIAL ##########################################
    html_texto_inicial = 'Please find bellow the daily report covering operating countries. <br/>You can also find weekly and monthly info on the links bellow: <br/>https://tripda.bime.io/dashboard/A2530F83565959CFACE3ACCEB3BC363C11873F8029AAC68A7662AE37AC40A2F5#/tab/503470 <br/>\
    https://tripda.bime.io/dashboard/7F884041D2B99BA53850093FF2CC9482A67D78713EA0EC46B1F5C5D5752EC7F3 <br/>\
    If there are any problems with the above address, please let me know. <br/>\
    Also, please let me know if you need any additional data or help reading the report <br/><br/><br/>'

    ### TITULO ##########################################
    html_titulo = '<table border="1"; style="font-size:30px"; align="center"; width="1100"; height="55";>\
    <th>Tripda Report - Daily</th></table><br/><br/>'

    ### CORPO ##########################################
    html_corpo = '<table border="1" style="font-size:12px; border-collapse: collapse;" width="1100"> \
    <style> table, th, td {border: 1px solid black;} </style>'

    html_corpo += '<tr height="40"> \
        <th colspan="2"></th> \
        <th bgcolor="#18BD9D" colspan="6" style="border-width: 2px;"><font color = "white">Traffic Activity</font></th> \
        <th bgcolor="#18BD9D" colspan="7" style="border-width: 2px;"><font color = "white">Realized View</font></th> \
    </tr>\n'

    html_corpo += '<tr bgcolor="#36E6C4" colspan="2"  height="60" style="border: solid 2px">'
    for i in headerQuery:
        if i == 'report_date':
            html_corpo += '   <th width="90" style="border-width: 1px 2px 1px 1px ;">'
        elif (i == 'ask'):
            html_corpo += '   <th width="60" style="border-width: 1px 2px 1px 1px ;">'
        elif (i == 'Pax_Cancel_Rate'):
            html_corpo += '   <th style="border-width: 1px 2px 1px 1px ;">'
        else:
            html_corpo += '   <th>'
        html_corpo += i.replace("_"," ")
        html_corpo += '</th>'
    html_corpo += '</tr>'

    iCounter = 0
    iCounterBoldTotal = 0
    bFlagRowGrey = False
    bFlagBottomLine = False

    for i in rows:
        if bFlagRowGrey == True: 
            if bFlagBottomLine == True:
                if iCounterBoldTotal < 4:
                    html_corpo += '<tr bgcolor="#EEEEEE" style="border-bottom: 2px solid black; font-weight: bold; font-size:13px">\n'
                    iCounterBoldTotal += 1
                else:
                    html_corpo += '<tr bgcolor="#EEEEEE" style="border-bottom: 2px solid black;">\n'
                bFlagBottomLine = False
            else:
                if iCounterBoldTotal < 4:
                    html_corpo += '<tr bgcolor="#EEEEEE" style="font-weight: bold; font-size:13px">\n'
                    iCounterBoldTotal += 1
                else:
                    html_corpo += '<tr bgcolor="#EEEEEE">\n'
                bFlagBottomLine = True
            bFlagRowGrey = False
        else:
            if iCounterBoldTotal < 4:
                html_corpo += '<tr style="font-weight: bold; font-size:13px">\n'
                iCounterBoldTotal += 1
            else:
                html_corpo += '<tr>\n'
            bFlagRowGrey = True

        for key in headerQuery:
            if key == 'country' and (iCounter in [0,4,8,12,16,20,24,28,32,36,40,44,48,52,56,60]) == False:
                html_corpo += ''
            elif key == 'country' and (iCounter in [0,4,8,12,16,20,24,28,32,36,40,44,48,52,56,60]) == True:
                html_corpo += '   <td align="center"; rowspan="4"; style="border-width: 2px 1px 2px 2px ;">'
                html_corpo += str(arr[key][i])
                html_corpo += '</td>\n'
            else:
                if (key in ['report_date','ask','Pax_Cancel_Rate']) == True:
                    html_corpo += '   <td align="center"; style="border-width: 1px 2px 1px 1px ;">'
                else:
                    html_corpo += '   <td align="center";>'

                if (key in index_rate_format) == True:
                    html_corpo += rate_format(arr[key][i])
                elif (key in index_average_format) == True:
                    html_corpo += average_format(arr[key][i])
                elif (key in index_default_format) == True:
                    html_corpo += default_format(arr[key][i])
                else:
                    html_corpo += str(arr[key][i])
                html_corpo += '</td>\n'
        iCounter += 1
        html_corpo += '</tr>\n'
    html_corpo += '</table> <br/>'

    ### LEGENDA GERAL ##########################################
    html_legenda_geral = '<table border="1" style="font-size:12px; border-collapse: collapse; border-width: 2px;" width="700">\n\
    <tr>  \n\
        <th bgcolor="#18BD9D" width="160"><font color = "white">Traffic activity view</font></th>\n\
        <td>Date of when users book or offer a ride on the site</td>\n\
    </tr>\n\
    <tr>  \n\
        <th bgcolor="#18BD9D"><font color = "white">Realized view</font></th>\n\
        <td>Date of when trips actually occurs</td>\n\
    </tr>\n \
    </table><br/>\n'

    ### LEGENDA DETALHADA 1 ##########################################
    html_legenda_detalhada = '<table border="1" style="font-size:12px; border-collapse: collapse; border-width: 2px;" width="1100">\n\
    \
    <tr>  \n\
        <th bgcolor="#18BD9D" rowspan="6"  width="120" align="center"><font color = "white">Traffic activity view</font></th>\n\
        <th bgcolor="#36E6C4" width="200">New Users</th>\n\
        <td>New users on the given period</td>\n\
    </tr>\n\
    \
    <tr>  \n\
        <th bgcolor="#36E6C4">New Drivers</th>\n\
        <td>New drivers on the given period</td>\n\
    </tr>\n\
    \
    <tr>  \n\
        <th bgcolor="#36E6C4">New Passangers</th>\n\
        <td>New passangers  on the given period</td>\n\
    </tr>\n\
    \
    <tr>  \n\
        <th bgcolor="#36E6C4">Trips Offered</th>\n\
        <td>Number of new trips offered in the given date. Number of trips don\'t include stopovers.\
    Eg: a trip from San Francisco to Los Angeles, stoping at Monterey and Santa Maria would count as 1 trip offered.</td>\n\
    </tr>\n\
    \
    <tr>  \n\
        <th bgcolor="#36E6C4">Bookings</th>\n\
        <td>Number of bookings in the given period. Eg: a booking of 3 seats booked on 12/09/2014 to travel on 14/09/2014 would be counted as 1 booking on 12/09/2014 </td>\n\
    </tr>\n\
    \
    <tr>  \n\
        <th bgcolor="#36E6C4">Available Seat Kilometer (ASK)</th>\n\
        <td>Number of seats offered in the given period times the number of kilometers of each trip.</td>\n\
    </tr>\n'

    ### LEGENDA DETALHADA 2 ##########################################
    html_legenda_detalhada += '<tr>  \n\
        <th bgcolor="#18BD9D" rowspan="7" align="center"><font color = "white">Realized view</font></th>\n\
        <th bgcolor="#36E6C4">Trips realized</th>\n\
        <td>Number of trips with at least one Tripda passenger in the given period, regardless the number of bookings in the car.\
    Eg: a trip from Bogota to Manizales with 3 PAX and 3 different bookings would caount as 1 trip.</td>\n\
    </tr>\n\
    \
    <tr>  \n\
        <th bgcolor="#36E6C4">PAX transported</th>\n\
        <td>Number os passengers transported in the given period</td>\n\
    </tr>\n\
    \
    <tr>  \n\
        <th bgcolor="#36E6C4">Average PAX per trip realized</th>\n\
        <td>Average PAX per trip realized  (PAX transported / Trips realized)</td>\n\
    </tr>\n\
    \
    <tr>  \n\
        <th bgcolor="#36E6C4">Realized TRIP Yield</th>\n\
        <td>Percentage of outstanding trips offered that converted into a realized trip, having at least one PAX transported.\
    Don\'t confuse with "Trips offered". Here we consider the date when the trip takes place. \
    Eg: if in one day Malaysia had 100 trips outstanding and 12 trips realized, the \"Realized trip yield\" would be 12%.</td>\n\
    </tr>\n\
    \
    <tr>  \n\
        <th bgcolor="#36E6C4">Average realized trip distance</th>\n\
        <td>Average distance traveled in the given period. The average is weighted by the number of passenger on each trip.</td>\n\
    </tr>\n\
    \
    <tr>  \n\
        <th bgcolor="#36E6C4">Driver Cancelation rate</th>\n\
        <td>Percentage of PAX affected by driver cancellations for trips that would happen in that given period.\
    If a Driver cancels a trip, in which no PAX has yet booked a seat, this wont\'t count for this statistics. \
    Eg: If there are 100 PAX travelig on Tripda in a given day and one driver cancels a ride with 4 seats booked, \
    the cacelation rate would be 4%</td>\n\
    </tr>\n\
    \
    <tr>  \n\
        <th bgcolor="#36E6C4">PAX Cancelation rate</th>\n\
        <td>Percentage of trips canceled by passengers that would actually become a real trip in the given period</td>\n\
    </tr>\n\
    \
    </table>'


    html = '<html>' + html_texto_inicial + html_titulo + html_corpo + html_legenda_geral + html_legenda_detalhada + '<br/><br/>Best regards, </html>'
    html_pdf = '<html>' + html_titulo + html_corpo + html_legenda_geral + html_legenda_detalhada + '</html>'

    if index == 1:
        return html
    else:  
        return html_pdf



def SendEmail(subject, msgText, to, user,password, alias, imgName, replyTo=None):
    sender = alias

    try:
        conn = SMTP('smtp.gmail.com', 587)

        msg = MIMEMultipart()
        msg.attach(MIMEText(msgText, 'html'))
        msg['Subject']= subject
        msg['From']   = sender
        msg['cc'] = to
        #msg['cc'] = ', '.join(to)

        if replyTo:
            msg['reply-to'] = replyTo

        if imgName != None:
            fp = open(imgName, 'rb')
            img = MIMEImage(fp.read(), _subtype="pdf")
            fp.close()
            img.add_header('Content-Disposition', 'attachment', filename = imgName)
            msg.attach(img)

        conn.ehlo()
        conn.starttls()
        conn.set_debuglevel(False)
        conn.login(user, password)
        try:
            conn.sendmail(sender, to, msg.as_string())
        finally:
            conn.close()
    except:
        print "Unexpected error:", sys.exc_info()[0]


def titles (index):
    import time
    from time import strftime

    def ord(n):
        return str(n)+("th" if 4<=n%100<=20 else {1:"st",2:"nd",3:"rd"}.get(n%10, "th"))

    yesterday = date.today() - timedelta(1)

    day = (yesterday.strftime("%d"))
    day = ord(int(day))
    month = (yesterday.strftime("%B"))

    Subject = "Tripda | Daily Report | " + month + " " + day

    day = yesterday.strftime("%d")
    month = yesterday.strftime("%m")
    year = yesterday.strftime("%y")

    PDFReport_Name = "New Daily Report " + month + "." + day + ".20" +  year + ".pdf"

    if index == 1 :
        return Subject
    else:
        return PDFReport_Name



#os.chdir("C:\Users\\ricardo.nakano\Desktop")


def main():
    log = open(DATA_DIR + "log", "a")
    log.write(str(datetime.now()) + "-START-DAILY\n")

    log.write(str(datetime.now()) +  "-START-DAILY-EXTRACT\n")
    db_table = extract()
    log.write(str(datetime.now()) +  "-END-DAILY-EXTRACT\n")

    log.write(str(datetime.now()) +  "-START-DAILY-TRANSFORM\n")
    daily_report = transform(db_table)
    log.write(str(datetime.now()) +  "-END-DAILY-TRANSFORM\n")

    log.write(str(datetime.now()) +  "-START-DAILY-SAVEPDF\n")
    html_pdf = html(daily_report,2)
    PDFReport_Name = titles(2)
    pdfkit.from_string(html_pdf, DATA_DIR + PDFReport_Name)
    log.write(str(datetime.now()) +  "-END-DAILY-SAVEPDF\n")

    log.write(str(datetime.now()) +  "-START-DAILY-SENDMAIL\n")
    html_body = html(daily_report,1)
    Subject = titles(1)
    SendEmail(Subject,html_body,'ricardo.nakano@tripda.com.br','tripda-datateam@tripda.com.br','123@datateam','',PDFReport_Name)
    SendEmail(Subject,html_body,'vitor.oliveira@ripda.com.br','tripda-datateam@tripda.com.br','123@datateam','',PDFReport_Name)
    log.write(str(datetime.now()) +  "-END-DAILY-SENDMAIL\n")

    log.write(str(datetime.now()) + "-END-DAILY\n")
    log.close()

if __name__ == "__main__":
    main()

