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

import pandasql

SQL_EXTRACT = '/home/voliveira/tripda-data-warehouse/bin/extract-bookings-table.sql'
DATA_DIR = "/home/rnakano/Cohort/"

# GLOBAL VARIABLES
countries = []
countries.append(' TOTAL')
countries.append('Argentina + Uruguay')
countries.append('Brasil')
countries.append('Chile')
countries.append('Colombia')
countries.append('India')
countries.append('Malaysia + Singapore')
countries.append('Mexico')
countries.append('Pakistan')
countries.append('Philippines')
countries.append('Taiwan')
countries.append('United States')

month = datetime.now().strftime("%m")
year = datetime.now().strftime("%Y")
months = []
actual_month = datetime.strptime('15' + month + year , "%d%m%Y").date()
actual_month = actual_month - timedelta(days=270)
for count in range(0,8):
    actual_month = actual_month + timedelta(days=30)
    months.append(actual_month)

reference_month = []
reference_month.append('same_month')
reference_month.append('1_month')
reference_month.append('2_month')
reference_month.append('3_month')
reference_month.append('4_month')
reference_month.append('5_month')
reference_month.append('6_month')
reference_month.append('7_to_12_month')

cohort_type = []
cohort_type.append('RR1')
cohort_type.append('RR1_Ac')
cohort_type.append('RR2')
cohort_type.append('RR2_Ac')

titles_main = ['Country','Month','Cohort size']
titles_cohort = ['(B) <br/> Same month','(C) <br/> 1 month','(D) <br/> 2 months','(E) <br/> 3 months', \
                 '(F) <br/> 4 months','(G) <br/> 5 months','(H) <br/> 6 months','(I) <br/> More than 6 months']
titles_name = []
titles_name.extend(titles_main)
titles_name.extend(titles_cohort)
titles_name.extend(titles_cohort)

def extract(index):
    try:
        conn = pymysql.connect(host='datateam-rds-ro.tripda.net', port=3306, user='rnakano', passwd='ze5QueL+', db='data_team',  charset='utf8')
    except Exception as e:
        print "Unable to connect to the database"
        print e

    if index == 1:
        extract_query = open(DATA_DIR + 'cohort_all_drivers_query.sql','r').read()
    elif index == 2:
        extract_query = open(DATA_DIR + 'cohort_first_driver_query.sql','r').read()
    elif index == 3:
        extract_query = open(DATA_DIR + 'cohort_all_passengers_query.sql','r').read()      
    elif index == 4:
        extract_query = open(DATA_DIR + 'cohort_first_passenger_query.sql','r').read()

    cursor = conn.cursor()

    db_table = psql.read_sql(extract_query, con=conn)  

    conn.close()
    return db_table

def transform_1(first_table):

    ##### Create a list for first time using Tripda per country and per month #####
    cohort_first = {}
    for locate in countries:
        cohort_first[locate] = {}
        for date in months:
            cohort_first[locate][date] = {}
            
            cohort_first[locate][date]['RR1'] = first_table[(first_table.country == locate) & \
                                         (first_table.year == date.year) & \
                                         (first_table.month == date.month)].user_id.tolist()

            cohort_first[locate][date]['RR2'] = first_table[(first_table.country == locate) & \
                                         (first_table.year == date.year) & \
                                         (first_table.month == date.month)].distance.tolist()

    return cohort_first

def transform_2(first_table, all_table, cohort_first):

    pysql = lambda q: pandasql.sqldf(q, globals())

    cohort_lists = {}

    for locate in countries:
        cohort_lists[locate] = {}
        
        for date in months:
            cohort_lists[locate][date] = {}
            acumulated_list = []
            
            if cohort_first[locate][date] == []:
                intermediary_query = "select distinct all_table.user_id from all_table where all_table.user_id in ('')"
            else:
                l = [str(x) for x in cohort_first[locate][date]['RR1']]
                intermediary_query = "select distinct all_table.user_id from all_table where all_table.user_id in (" + ','.join(l) + ")"

            month_query = intermediary_query + ' and all_table.year = ' + str(date.year) + ' and all_table.month = ' +  str(date.month)
            month_df = pandasql.sqldf(month_query, locals())
            month_list = month_df['user_id'].values.tolist()

            for reference in reference_month:
                print str(datetime.now()) + ' ==> ' + str(locate) + ' - ' + str(date) + ' - ' + str(reference)
                cohort_lists[locate][date][reference] = {}         

                if reference == 'same_month':
                    reference_date = date
                    same_month_query = 'all_table.year = ' + str(reference_date.year) + \
                                       ' and all_table.month = ' + str(reference_date.month) + \
                                       ' and all_table.count > 1'
                    
                    end_query = ' and ' + same_month_query
                    end_query_Ac = ' and ' + same_month_query
                
                Ac_month = actual_month - timedelta(days=30)
                if reference == '1_month' and Ac_month >= date:
                    reference_date = date + timedelta(days=30)
                    _1_month_query = 'all_table.year = ' + str(reference_date.year) + \
                                     ' and all_table.month = ' + str(reference_date.month)
                    
                    end_query = ' and ' + _1_month_query
                    end_query_Ac =  ' and ((' + same_month_query + ') or (' + _1_month_query + '))'

                Ac_month = actual_month - timedelta(days=60)
                if reference == '2_month' and Ac_month >= date:
                    reference_date = date + timedelta(days=60)
                    _2_month_query = 'all_table.year = ' + str(reference_date.year) + \
                                     ' and all_table.month = ' + str(reference_date.month)
                    
                    end_query = ' and ' + _2_month_query
                    end_query_Ac =  ' and ((' + same_month_query + ') or (' + _1_month_query + ') or (' + _2_month_query + '))'
                
                Ac_month = actual_month - timedelta(days=90)
                if reference == '3_month' and Ac_month >= date:
                    reference_date = date + timedelta(days=90)
                    _3_month_query = 'all_table.year = ' + str(reference_date.year) + \
                                     ' and all_table.month = ' + str(reference_date.month)
                    
                    end_query = ' and ' + _3_month_query
                    end_query_Ac =  ' and ((' + same_month_query + ') or (' + _1_month_query + ') or (' + _2_month_query + ') or (' \
                                        + _3_month_query + '))' 
                
                Ac_month = actual_month - timedelta(days=120)
                if reference == '4_month' and Ac_month >= date:
                    reference_date = date + timedelta(days=120)
                    _4_month_query = 'all_table.year = ' + str(reference_date.year) + \
                                ' and all_table.month = ' + str(reference_date.month)
                    
                    end_query = ' and ' + _4_month_query
                    end_query_Ac =  ' and ((' + same_month_query + ') or (' + _1_month_query + ') or (' + _2_month_query + ') or (' \
                                        + _3_month_query + ') or (' + _4_month_query + '))' 
                
                Ac_month = actual_month - timedelta(days=150)
                if reference == '5_month' and Ac_month >= date:
                    reference_date = date + timedelta(days=150)
                    _5_month_query = 'all_table.year = ' + str(reference_date.year) + \
                                ' and all_table.month = ' + str(reference_date.month)
                    
                    end_query = ' and ' + _5_month_query
                    end_query_Ac =  ' and ((' + same_month_query + ') or (' + _1_month_query + ') or (' + _2_month_query + ') or (' \
                                        + _3_month_query + ') or (' + _4_month_query + ') or (' + _5_month_query + '))' 
        
                Ac_month = actual_month - timedelta(days=180)
                if reference == '6_month' and Ac_month >= date:
                    reference_date = date + timedelta(days=180)
                    _6_month_query = 'all_table.year = ' + str(reference_date.year) + \
                                ' and all_table.month = ' + str(reference_date.month)
                    
                    end_query = ' and ' + _6_month_query
                    end_query_Ac =  ' and ((' + same_month_query + ') or (' + _1_month_query + ') or (' + _2_month_query + ') or (' \
                                        + _3_month_query + ') or (' + _4_month_query + ') or (' + _5_month_query + ') or (' + _6_month_query + '))' 

                Ac_month = actual_month - timedelta(days=210)
                if reference == '7_to_12_month' and Ac_month >= date:
                    reference_date = date + timedelta(days=210)
                    _7_to_12_month_query = '((all_table.year = ' + str(reference_date.year) + \
                                ' and all_table.month = ' + str(reference_date.month) + ')'
                    reference_date = date + timedelta(days=240)
                    _7_to_12_month_query += ' or (all_table.year = ' + str(reference_date.year) + \
                                ' and all_table.month = ' + str(reference_date.month) + ')'
                    reference_date = date + timedelta(days=270)
                    _7_to_12_month_query += ' or (all_table.year = ' + str(reference_date.year) + \
                                ' and all_table.month = ' + str(reference_date.month) + ')'
                    reference_date = date + timedelta(days=300)
                    _7_to_12_month_query += ' or (all_table.year = ' + str(reference_date.year) + \
                                ' and all_table.month = ' + str(reference_date.month) + ')'
                    reference_date = date + timedelta(days=330)
                    _7_to_12_month_query += ' or (all_table.year = ' + str(reference_date.year) + \
                                ' and all_table.month = ' + str(reference_date.month) + ')'
                    reference_date = date + timedelta(days=360)
                    _7_to_12_month_query += ' or (all_table.year = ' + str(reference_date.year) + \
                                ' and all_table.month = ' + str(reference_date.month) + '))'

                    end_query = ' and ' + _7_to_12_month_query
                    end_query_Ac =  ' and ((' + same_month_query + ') or (' + _1_month_query + ') or (' + _2_month_query + ') or (' \
                                        + _3_month_query + ') or (' + _4_month_query + ') or (' + _5_month_query + ') or (' + _6_month_query + ') or (' \
                                        + _7_to_12_month_query + '))'

                auxiliar_query = intermediary_query + end_query

                auxiliar_df = pandasql.sqldf(auxiliar_query, locals()) 
                auxiliar_list = auxiliar_df['user_id'].values.tolist()

                #List of accumulated users on the referent date
                acumulated_list.extend(auxiliar_list)
                acumulated_list = list(set(acumulated_list))

                ml = [str(x) for x in month_list]
                al = [str(x) for x in acumulated_list]
       
                if ml == []:
                    RR1_query = "select count(distinct all_table.user_id) from all_table where all_table.user_id in ('')"
                    RR2_query = "select sum(all_table.distance) from all_table where all_table.user_id in ('')"
                else:
                    RR1_query = "select count(distinct all_table.user_id) from all_table where all_table.user_id in (" + ','.join(ml) + ')'
                    RR2_query = "select sum(all_table.distance) from all_table where all_table.user_id in (" + ','.join(ml) + ')'

                if al == []:
                    RR1Ac_query = "select count(distinct all_table.user_id) from all_table where all_table.user_id in ('')"
                    RR2Ac_query = "select sum(all_table.distance) from all_table where all_table.user_id in ('')"
                else:
                    RR1Ac_query = "select count(distinct all_table.user_id) from all_table where all_table.user_id in (" + ','.join(al) + ')'
                    RR2Ac_query = "select sum(all_table.distance) from all_table where all_table.user_id in (" + ','.join(al) + ')'              
                 
                RR1_query += end_query
                RR1Ac_query += end_query_Ac
                RR2_query += end_query
                RR2Ac_query += end_query_Ac

                default_format = lambda x: str(x).translate(None, ''.join(['[' , ']' , '.', ' ']))
                
                cohort_lists[locate][date][reference] = {}
                
                final_df = pandasql.sqldf(RR1_query, locals())
                if default_format(final_df.values) == 'None':
                    cohort_lists[locate][date][reference]['RR1'] = 0
                else:
                    cohort_lists[locate][date][reference]['RR1'] = default_format(final_df.values)

                final_df = pandasql.sqldf(RR1Ac_query, locals()) 
                if default_format(final_df.values) == 'None':
                    cohort_lists[locate][date][reference]['RR1_Ac'] = 0
                else:
                    cohort_lists[locate][date][reference]['RR1_Ac'] = default_format(final_df.values)
                
                final_df = pandasql.sqldf(RR2_query, locals()) 
                if default_format(final_df.values) == 'None':
                    cohort_lists[locate][date][reference]['RR2'] = 0
                else:
                    cohort_lists[locate][date][reference]['RR2'] = default_format(final_df.values)
                
                final_df = pandasql.sqldf(RR2Ac_query, locals()) 
                if default_format(final_df.values) == 'None':
                    cohort_lists[locate][date][reference]['RR2_Ac'] = 0
                else:
                    cohort_lists[locate][date][reference]['RR2_Ac'] = default_format(final_df.values)
                    
                    
    for locate in countries:
        for date in months:
            for reference in reference_month:
                for cohort in cohort_type:
                    cohort_lists[' TOTAL'][date][reference][cohort] =  str(int(cohort_lists[' TOTAL'][date][reference][cohort]) +
                                                                        int(cohort_lists[locate][date][reference][cohort]))

    return cohort_lists

def transform_3(cohort_first):

    cohort_size = {}
    for locate in countries:
        cohort_size[locate] = {}
        
        for date in months:
            cohort_size[locate][date] = {}

            cohort_size[locate][date]['RR1'] = len(cohort_first[locate][date]['RR1'])
            cohort_size[locate][date]['RR2'] = sum([int(x) for x in cohort_first[locate][date]['RR2']])
            cohort_size[' TOTAL'][date]['RR1'] += cohort_size[locate][date]['RR1']

    return cohort_size

def transform_4(cohort_lists):
    for country in countries:
        for cohort in cohort_type:
            
            counter = 8
            for reference in reference_month:
                counter -= 1
                aux = counter
                for date in months:
                    
                    if aux >= 0:
                        cohort_lists[country][date][reference][cohort] = cohort_lists[country][date][reference][cohort]
                        aux -= 1
                    else:
                        cohort_lists[country][date][reference][cohort] = -100000

    return cohort_lists

def html_inicial():

    ### TEXTO INICIAL ##########################################
    
    html_texto_inicial = 'Please find bellow the daily report covering operating countries.\n <br/>You can also find weekly and monthly info on the links bellow:\n <br/> \
    https://tripda.bime.io/dashboard/A2530F83565959CFACE3ACCEB3BC363C11873F8029AAC68A7662AE37AC40A2F5#/tab/503470 \n <br/>\
    https://tripda.bime.io/dashboard/7F884041D2B99BA53850093FF2CC9482A67D78713EA0EC46B1F5C5D5752EC7F3 \n <br/>\
    If there are any problems with the above address, please let me know. \n<br/>\
    Also, please let me know if you need any additional data or help reading the report <br/><br/><br/>\n\n'
    
    return html_texto_inicial

def html (cohort_lists, cohort_size, index):

    import pandas
    import numpy as np

    ### TITULO ##########################################
    if index == 1:
        html_titulo = '\n <br/> <table border="1"; style="font-size:30px"; align="center"; width="1100"; height="55";>\
    <th>Cohort - Driver</th></table><br/><br/>\n\n'
    else:
        html_titulo = '\n <br/> <table border="1"; style="font-size:30px"; align="center"; width="1100"; height="55";>\
    <th>Cohort - Passenger</th></table><br/><br/>\n\n'        
    ### CORPO 1 (RR1)##########################################
    rate_format = lambda x: str('%.1f' % x) + '%'

    html_corpo = '<table border="1" style="font-size:12px; border-collapse: collapse;" width="1100" align="center"> \
    <style> table, th, td {border: 1px solid black;} </style>\n'

    html_corpo += '    <tr> \n\
            <th colspan="3"></th> \n\
            <th bgcolor="#18BD9D" colspan="8" style="border-width: 2px;"><font color = "white">RR1</font></th> \n\
            <th bgcolor="#18BD9D" colspan="8" style="border-width: 2px;"><font color = "white">RR1 Acummulated</font></th> \n\
        </tr>\n\n'

    html_corpo += '    <tr bgcolor="#36E6C4" colspan="2"  height="60" style="border: solid 2px; font-size:9px">\n'

    for title in titles_name:
        if title == 'Month':
            html_corpo += '       <th width="200" style="border-width: 1px 2px 1px 1px ;">'
        else:
            html_corpo += '       <th width="90" style="border-width: 1px 2px 1px 1px ;">'
        html_corpo += title
        html_corpo +='</th>\n'
    html_corpo += '    </tr>\n\n'

    ref_country = ''
    for country in countries:
        for date in months:
            if country == ref_country:
                html_corpo += '    <tr>\n'
                html_corpo += ''
            else:
                html_corpo += '    <tr style="border-top: 2px solid black;">\n'
                html_corpo += '       <td align="center"; rowspan="8";>' + country + '</td>\n'
                ref_country = country
            html_corpo += '       <td align="center";>' + date.strftime("%b %Y") + '</td>\n'
            html_corpo += '       <td align="center"; bgcolor="#EEEEEE">' + str(cohort_size[country][date]['RR1']) + '</td>\n'
            for cohort in ('RR1','RR1_Ac'):
                for reference in reference_month:
                    if reference == '7_to_12_month':
                        if cohort_size[country][date]['RR1'] == 0 or cohort_lists[country][date][reference][cohort] < 0:
                            html_corpo += '       <td align="center"; style="border-width: 1px 2px 1px 1px ;">''</td>\n'
                        else:
                            html_corpo += '       <td align="center"; style="border-width: 1px 2px 1px 1px ;">' + rate_format(100*float(cohort_lists[country][date][reference][cohort])/cohort_size[country][date]['RR1']) + '</td>\n'
                    else:
                        if cohort_size[country][date]['RR1'] == 0 or cohort_lists[country][date][reference][cohort] < 0:
                            html_corpo += '       <td align="center";>''</td>\n'
                        else:
                            html_corpo += '       <td align="center";>' + rate_format(100*float(cohort_lists[country][date][reference][cohort])/cohort_size[country][date]['RR1']) + '</td>\n'
            html_corpo += '    </tr>\n'

    html_corpo += '</table>\n <br/><br/>'        


    ### CORPO 2 (RR2)##########################################

    html_corpo += '\n\n<table border="1" style="font-size:12px; border-collapse: collapse;" width="1100" align="center"> \
    <style> table, th, td {border: 1px solid black;} </style>\n'

    html_corpo += '    <tr> \n\
            <th colspan="3"></th> \n\
            <th bgcolor="#18BD9D" colspan="8" style="border-width: 2px;"><font color = "white">RR2</font></th> \n\
            <th bgcolor="#18BD9D" colspan="8" style="border-width: 2px;"><font color = "white">RR2 Acummulated</font></th> \n\
        </tr>\n\n'

    html_corpo += '    <tr bgcolor="#36E6C4" colspan="2"  height="60" style="border: solid 2px; font-size:9px">\n'

    for title in titles_name:
        if title == 'Month':
            html_corpo += '       <th width="200" style="border-width: 1px 2px 1px 1px ;">'
        else:
            html_corpo += '       <th width="90" style="border-width: 1px 2px 1px 1px ;">'
        html_corpo += title
        html_corpo +='</th>\n'
    html_corpo += '    </tr>\n\n'

    ref_country = ''
    for country in countries:
        for date in months:
            if country == ref_country:
                html_corpo += '    <tr>\n'
                html_corpo += ''
            else:
                html_corpo += '    <tr style="border-top: 2px solid black;">\n'
                html_corpo += '       <td align="center"; rowspan="8";>' + country + '</td>\n'
                ref_country = country
            html_corpo += '       <td align="center";>' + date.strftime("%b %Y") + '</td>\n'
            html_corpo += '       <td align="center"; bgcolor="#EEEEEE">' + str(cohort_size[country][date]['RR2']) + '</td>\n'
            for cohort in ('RR2','RR2_Ac'):
                for reference in reference_month:
                    if reference == '7_to_12_month':
                        if cohort_size[country][date]['RR2'] == 0 or cohort_lists[country][date][reference][cohort] < 0:
                             html_corpo += '       <td align="center"; style="border-width: 1px 2px 1px 1px ;">''</td>\n'
                        else:
                            html_corpo += '       <td align="center"; style="border-width: 1px 2px 1px 1px ;">' + rate_format(100*float(cohort_lists[country][date][reference][cohort])/(cohort_size[country][date]['RR2'])) + '</td>\n'
                    else:
                        if cohort_size[country][date]['RR2'] == 0 or cohort_lists[country][date][reference][cohort] < 0:
                            html_corpo += '       <td align="center";>''</td>\n'
                        else:
                            html_corpo += '       <td align="center";>' + rate_format(100*float(cohort_lists[country][date][reference][cohort])/(cohort_size[country][date]['RR2'])) + '</td>\n'
            html_corpo += '    </tr>\n'

    html_corpo += '</table> \n <br/>'

    html_body = html_titulo + html_corpo
    
    return html_body

def html_final():

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


    html_legend = html_legenda_geral + html_legenda_detalhada


    return html_legend

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

    month = (date.today().strftime("%B"))
    Subject = "Tripda - Pax Cohort Report - " + month
    
    month = date.today().strftime("%B")
    year = date.today().strftime("%y")
    PDFReport_Name = "Cohorts " + month + " 20" +  year + ".pdf"

    if index == 1 :
        return Subject
    else:
        return PDFReport_Name

def main():
    log = open(DATA_DIR + "log_cohort", "a")
    log.write(str(datetime.now()) + "-START-COHORT\n")
    # EXTRACT ######################################################################## EXTRACT #
    log.write(str(datetime.now()) +  "-START-COHORT-EXTRACT\n")

    all_drivers_table = extract(1)
    first_driver_table = extract(2)
    all_pax_table = extract(3)
    first_pax_table = extract(4)
    all_drivers_table.to_csv(DATA_DIR + 'all_drivers_table', sep='\t')
    first_driver_table.to_csv(DATA_DIR + 'first_driver_table', sep='\t')
    all_pax_table.to_csv(DATA_DIR + 'all_pax_table', sep='\t')
    first_pax_table.to_csv(DATA_DIR + 'first_pax_table', sep='\t')

    log.write(str(datetime.now()) +  "-END-COHORT-EXTRACT\n")
    # TRANSFORM ######################################################################## TRANSFORM #
    log.write(str(datetime.now()) +  "-START-COHORT-TRANSFORM\n")

    driver_first_list = transform_1(first_driver_table)
    pax_first_list = transform_1(first_pax_table)

    cohort_driver = transform_2(first_driver_table, all_drivers_table, driver_first_list)
    cohort_passenger = transform_2(first_pax_table, all_pax_table, pax_first_list)

    cohort_size_driver = transform_3(driver_first_list)
    cohort_size_pax = transform_3(pax_first_list)

    cohort_driver_final = transform_4(cohort_driver)
    cohort_passenger_final = transform_4(cohort_passenger)

    log.write(str(datetime.now()) +  "-END-COHORT-TRANSFORM\n")
    # HTML ################################################################################## HTML #
    log.write(str(datetime.now()) +  "-START-COHORT-HTML\n")

    html_driver = html(cohort_driver_final,cohort_size_driver,1)
    html_pax = html(cohort_passenger_final,cohort_size_pax,2)

    log.write(str(datetime.now()) +  "-END-COHORT-HTML\n")
    # SAVE PDF ############################################################################ SAVE PDF #
    log.write(str(datetime.now()) +  "-START-COHORT-SAVEPDF\n")

    html_pdf = html_driver + html_pax + html_final()
    PDFReport_Name = titles(2)
    pdfkit.from_string(html_pdf, DATA_DIR + PDFReport_Name)

    log.write(str(datetime.now()) +  "-END-COHORT-SAVEPDF\n")
    # SEND MAIL ########################################################################### SEND MAIL #
    log.write(str(datetime.now()) +  "-START-COHORT-SENDMAIL\n")

    html_body = html_inicial() + html_driver + html_pax + html_final()
    Subject = titles(1)
    SendEmail(Subject,html_body,'ricardo.nakano@tripda.com.br','tripda-datateam@tripda.com.br','123@datateam','',PDFReport_Name)

    log.write(str(datetime.now()) +  "-END-COHORT-SENDMAIL\n")
    ########################################################################
    log.write(str(datetime.now()) + "-END-COHORT\n")
    log.close()

if __name__ == "__main__":
    main()

