from ga_connector import ga_connect
from sqlalchemy import create_engine
import datetime
import pandas
from pandas import DataFrame as pd
from pandas import DataFrame as df
import pandas_gbq
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
from googleapiclient.discovery import build
import gspread

key_path = '/home/web_analytics/m2-main-cd9ed0b4e222.json'
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)


def date_pairs(date1, date2, step= 1):
    pairs= []
    while date2 > date1:
        prev_date = date2 - datetime.timedelta(days=step-1) if date2 - datetime.timedelta(days=step) > date1 else date1
        pair = [str(prev_date), str(date2)]   
        date2 -= datetime.timedelta(days=step)
        pairs.append(pair)
    pairs.reverse()
    return pairs

def get_df(query): 
    engine = create_engine(postrges_key)
    df_check = engine.execute(query)
    df_q = pd(df_check.fetchall(), columns = df_check.keys())
    engine.dispose()
    return df_q

def get_df_new(query):
    df_check = engine.execute(query)
    return df(df_check.fetchall(), columns = df_check.keys()).fillna(0)

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
    
def get_product(s):
    if 'protection' in s.lower() or 'guarantee' in s.lower():
        return 'protection'
    if 'onlinebuy' in s.lower():
        return 'onlinebuy'
    else:
        return None
    
def get_type(s):
    
    if 'click' in s.lower():
        return 'click'
    if 'view' in s.lower():
        return 'view'
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
        
def get_df(query):
    df_check = engine.execute(query)
    return df(df_check.fetchall(), columns = df_check.keys()).fillna(0)

def purify_df(df):
    df.columns = [i.replace('ga:', '').lower() for i in df.columns]
    df['product'] = df['eventlabel'].apply(get_product)
    df['card_serp'] = df['eventlabel'].apply(get_location)
    df['action_type'] = df['eventlabel'].apply(get_type)
    df = df.drop(columns = ['eventcategory','eventaction'])
    df['totalevents'] = df['totalevents'].astype(int)
    df['users'] = df['users'].astype(int)
    df = df[df['product'].apply(lambda x: x is not None)]
    return df


start_date_tr = """SELECT  MAX(DATE) as date FROM `m2-main.UA_REPORTS.MISC_UP_Banners` """
start_date_tr  = pandas_gbq.read_gbq(start_date_tr, project_id='m2-main', credentials=gbq_credential)
start_date_tr  = (datetime.datetime.strptime(start_date_tr['date'][0],"%Y-%m-%d").date()+datetime.timedelta(days=1))     

date1 = start_date_tr
end = (datetime.datetime.today() -datetime.timedelta(days=1)).date()
dates_couples = date_pairs(date1, end, step=1)
date1

ga_conc = ga_connect('208464364')
params = {'dimetions': [{'name': 'ga:isoweek'},
                        {'name': 'ga:date'},
                        {'name': 'ga:eventLabel'},
                        {'name': 'ga:eventCategory'},
                        
                        {'name': 'ga:eventAction'}
                       ],
          
        'metrics':[{'expression': 'ga:totalevents'},
                    {'expression': 'ga:users'}
                  ],
          
        
        'filters':'ga:pagepath=~/nedvizhimost;ga:eventLabel=~banner'
        }

UP_rep = ga_conc.report_pd(dates_couples,params)


UP_rep.to_gbq(f'UA_REPORTS.MISC_UP_Banners', project_id='m2-main', if_exists='append', credentials=gbq_credential)


a = ''' SELECT * FROM m2-main.UA_REPORTS.MISC_UP_Banners where date >= '2022-01-03' '''
aa = pandas_gbq.read_gbq(a, project_id='m2-main', credentials=gbq_credential)
event_banner_df = purify_df(aa)

engine = create_engine('sqlite://')

event_banner_df.to_sql("banners", con=engine, if_exists = 'replace', index = False)
q = '''
select 
    isoweek,
    product,
    action_type,
    card_serp,
    sum(totalevents) as bnrs,
    sum(users) as un_users
from 
    banners

group by 1,2,3,4
order by 1
'''
get_bans = get_df_new(q)




SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
g_credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)


gc = gspread.authorize(g_credentials)

rows  = [list([get_bans.columns][0])]

for i in get_bans.itertuples():
    
    if i.isoweek == 0:
        continue
    else:
        ls=list(i)[1:]
        rows.append(ls)

banners_list = rows
        
sh = gc.open("1. РК Траффик 2022 Total")
wk = sh.worksheet('source_all')
wk.update('Y2',banners_list)

# ГАРАНТИЯ СДЕЛОК И ПР.

date1 = datetime.date(2021,1,3) # НЕ менять
end = (datetime.datetime.today()-datetime.timedelta(days=1)).date()
dates_couples = date_pairs(date1,end, step=1)

ga_conc = ga_connect('208464364')

params = {'dimetions': [{'name': 'ga:isoweek'},
                        {'name': 'ga:eventLabel'},
                        {'name': 'ga:eventCategory'},
                        {'name': 'ga:dimension1'}
                       ],
        'metrics':[{'expression': 'ga:totalevents'},
                    {'expression': 'ga:users'}
                  ],
        'filters':'ga:landingpagepath=~nedvizhimost;ga:eventLabel=~lk_onlinebuy_lp_header_buy_click'
        }

od_lk_banners = ga_conc.report_pd(dates_couples,params)

params = {'dimetions': [{'name': 'ga:isoweek'},
                        {'name': 'ga:pagepath'},
                        {'name': 'ga:dimension1'},
                        {'name': 'ga:dimension2'}
                       ],
        'metrics':[
                    {'expression': 'ga:users'}
                  ],
        'filters':'ga:landingpagepath=~nedvizhimost;ga:pagepath=~services/deal'
        }

od_users = ga_conc.report_pd(dates_couples,params)



params = {'dimetions': [{'name': 'ga:isoweek'},
                        {'name': 'ga:pagepath'},
                        {'name': 'ga:dimension1'},
                        {'name': 'ga:dimension2'}
                       ],
        'metrics':[
                    {'expression': 'ga:users'}
                  ],
        'filters':'ga:landingpagepath=~nedvizhimost;ga:pagepath=~guaranteed-deal'
        }

garant_users = ga_conc.report_pd(dates_couples,params)


params = {'dimetions': [{'name': 'ga:isoweek'},
                        {'name': 'ga:eventLabel'},
                        {'name': 'ga:eventCategory'},
                        {'name': 'ga:dimension1'}
                       ],
        'metrics':[{'expression': 'ga:totalevents'},
                    {'expression': 'ga:users'}
                  ],
        'filters':'ga:landingpagepath=~nedvizhimost;ga:eventLabel=~gd_start'
        }

garant_lk_banners = ga_conc.report_pd(dates_couples,params)


engine = create_engine('sqlite://')
od_lk_banners.to_sql("OD_LK", con=engine, if_exists = 'replace', index = False)
od_users.to_sql("OD_USERS", con=engine, if_exists = 'replace', index = False)
garant_lk_banners.to_sql("GD_LK", con=engine, if_exists = 'replace', index = False)
garant_users.to_sql("GD_USERS", con=engine, if_exists = 'replace', index = False)

def get_df(query, engine = engine):
    df_check = engine.execute(query)
    return pandas.DataFrame(df_check.fetchall(), columns = df_check.keys()).fillna(0)

q = '''

with od_u_pull as 
(select  isoweek, count(distinct dimension1) AS od_us from OD_USERS where dimension2 != 'not_authorized' GROUP by 1),

gd_u_pull as 
(select isoweek, count(distinct dimension1) AS gd_us from GD_USERS where dimension2 != 'not_authorized' GROUP by 1),

od_lp_click as
(select isoweek,  sum(users) as od_clicks from OD_LK GROUP by 1),

gd_lp_click as 
(select isoweek,  sum(users) as gd_clicks from GD_LK GROUP by 1),

week as 
(select distinct isoweek as isoweek from GD_USERS)


SELECT 
week.isoweek as isoweek,

od_us,
gd_us,
od_clicks,
gd_clicks


FROM week

 LEFT JOIN od_u_pull on 
od_u_pull.isoweek = week.isoweek

LEFT JOIN gd_u_pull on 
gd_u_pull.isoweek = week.isoweek

LEFT JOIN od_lp_click on 
od_lp_click.isoweek = week.isoweek

LEFT JOIN gd_lp_click on 
gd_lp_click.isoweek = week.isoweek


'''
misc_cross_events = get_df(q)



rows  = [list([misc_cross_events.columns][0])]

for i in misc_cross_events.itertuples():
    
    if i.isoweek == 0:
        continue
    else:
        ls=list(i)[1:]
        rows.append(ls)


sh = gc.open("1. РК Траффик 2022 Total")
wk = sh.worksheet('source_all')
wk.update('AF2',rows)
