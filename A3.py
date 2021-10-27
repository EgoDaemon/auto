import datetime
import os
from sqlalchemy import create_engine
import pandas_gbq
import requests
import json
import pandas
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import gspread
import string
import pandas as pd
from ga_connector import ga_connect
from pandas import DataFrame as df
from googleapiclient.discovery import build

key_path = 'C:\\Users\\messnerav\\projects\\m2-main-cd9ed0b4e222.json'
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)

def sheet_ready(df_r):
    rows  = [list(df_r.columns)]
    for i in df_r.itertuples():
        ls=list(i)[1:]
        rows.append(ls)
    return rows

def date_pairs(date1, date2, step= 1):
    pairs= []
    while date2 >= date1:
        prev_date = date2 - datetime.timedelta(days=step-1) if date2 - datetime.timedelta(days=step) >= date1 else date1
        pair = [str(prev_date), str(date2)]   
        date2 -= datetime.timedelta(days=step)
        pairs.append(pair)
    pairs.reverse()
    return pairs

def get_cpc_soures(cpms):
    if 'second' in cpms:
        return 'УП реклама'
    if 'new-building' in cpms:
        return 'Новосторойки реклама'
    if cpms == '(not set)':
        return 'Не рекламные'
    else:
        return 'Другие рекламные кампании'
    
def get_location(s):
    if 'serp' in s.lower():
        return 'SERP'
    if 'card' in s.lower():
        return 'CARD'
    else:
        return None 
    
def get_high(s):
    if 'bottom' in s.lower():
        return 'bottom'
    if 'top' in s.lower():
        return 'top'
    else:
        return None     

def get_geo(page):
    
    msk='moskva-i-oblast|moskva|moskovskaya-oblast'.split('|')
    spb= 'sankt-peterburg|leningradskaya-oblast|sankt-peterburg-i-oblast'.split('|')
    for g in msk:
        if g in page:
            return "MSK"
    for g in spb:
        if g in page:
            return "SPB"
        
def purify_df(df):
    df.columns = [i.replace('ga:', '') for i in cl_calls_df.columns]
    df = df[df['eventlabel'].apply(lambda x: 'Rent' not in x)]
    df['source'] = df['sourcemedium'].apply(lambda x: x.split(" / ")[0])
    df['medium'] = df['sourcemedium'].apply(lambda x: x.split(" / ")[1])
    df['cpc_type'] = df['campaign'].apply(get_cpc_soures)
    df['VAS_type'] = df['eventlabel'].apply(lambda x: "VAS" if 'vas' in x.lower() else "NOT_VAS")
    df['CARD_SERP'] = df['eventlabel'].apply(get_location)
    df['TOP_BOTTOM'] = df['eventlabel'].apply(get_high)
    df['city'] = df['pagepath'].apply(get_geo)
    df['week'] = df['date'].apply(lambda x: x.isocalendar()[1])
    df = df.drop(columns = ['sourcemedium','pagepath'])
    df['totalEvents'] = df['totalEvents'].astype(int)
    df['uniqueEvents'] = df['uniqueEvents'].astype(int)
    
    return df

q = """SELECT  MAX(DATE) as date FROM `m2-main.UA_REPORTS.A3` """

last_dt1 = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
last_dt1 = (datetime.datetime.strptime(last_dt1['date'][0],"%Y-%m-%d").date()+datetime.timedelta(days=1))     
last_dt1

date1 = last_dt1
end = (datetime.datetime.today() -datetime.timedelta(days=1)).date()
dates_couples = date_pairs(date1,end, step=4)
    

ga_conc = ga_connect('208464364')

filtr = 'ga:eventlabel=~^Cl.*PhoneClick.*'


params = {'dimetions':  [{'name': 'ga:date'},
                         {'name': 'ga:eventlabel'},
                         {'name': 'ga:campaign'},
                         {'name': 'ga:sourcemedium'},
                         {'name': 'ga:pagepath'},
                         {'name': 'ga:deviceCategory'},
                         
                         
                         
                        ],
        'metrics':[{'expression': 'ga:totalEvents'},
                   {'expression': 'ga:uniqueEvents'}
                  ],
        
        'filters': filtr
        }


cl_calls_df = ga_conc.report_pd(dates_couples,params)
pure_df = purify_df(cl_calls_df)

engine = create_engine('sqlite://')
pure_df.to_sql("nbs", con=engine, if_exists = 'replace', index = False)

def get_df(query, engine = engine):
    df_check = engine.execute(query)
    return df(df_check.fetchall(), columns = df_check.keys()).fillna(0)

q = '''
select 
    date(date) as date , eventlabel, campaign, deviceCategory, source, medium, cpc_type, VAS_type, 
    CARD_SERP, TOP_BOTTOM, city, week,
    sum(totalEvents) as totalEvents,
    sum(uniqueEvents) as uniqueEvents
from nbs
group by 1,2,3,4,5,6,7,8,9,10,11,12
'''
hlops = get_df(q)
engine.dispose()

hlops.to_gbq(f'UA_REPORTS.A3', project_id='m2-main', if_exists='append', credentials=gbq_credential)

q = """SELECT week, city, eventlabel, source, medium, cpc_type, VAS_type, CARD_SERP, TOP_BOTTOM,deviceCategory, 
sum(totalEvents) as tot_event, sum(uniqueEvents) as u_event

FROM m2-main.UA_REPORTS.A3

group by  1,2,3,4,5,6,7,8,9,10
order by 1"""

all_clops=pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('m2ru-322008-49d82df3892d.json', SCOPES)
gc = gspread.authorize(credentials)

all_clops['tot_event'] = all_clops['tot_event'].astype(int)
all_clops['u_event'] = all_clops['u_event'].astype(int)

sh = gc.open("1. РК Траффик 2020/21 Total")
wk = sh.worksheet('All_clpos')

g_clop=sheet_ready(all_clops)
wk.update('A1',g_clop)

sh = gc.open("План/Факт Маркетинг по Классифайду (недельный)")
wk = sh.worksheet('vas_source')

g_clop=sheet_ready(all_clops)
wk.update('A1',g_clop)