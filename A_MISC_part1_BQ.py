from ga_connector import ga_connect
from sqlalchemy import create_engine
import datetime
import pandas
from pandas import DataFrame as pd
from pandas import DataFrame as df
import pandas_gbq
from ga_connector import ga_connect
from sqlalchemy import create_engine
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import gspread
from isoweek import Week

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
    engine = create_engine(key_path)
    df_check = engine.execute(query)
    df_q = pd(df_check.fetchall(), columns = df_check.keys())
    engine.dispose()
    return df_q


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
    df['landingpagepath'] = df['landingpagepath'].apply(get_geo)
    return df

start_date_tr = """SELECT  MAX(DATE) as date FROM `m2-main.UA_REPORTS.MISC_Transfer_broker` """
start_date_tr  = pandas_gbq.read_gbq(start_date_tr, project_id='m2-main', credentials=gbq_credential)
start_date_tr  = (datetime.datetime.strptime(start_date_tr['date'][0],"%Y-%m-%d").date()+datetime.timedelta(days=1))     

date1 = start_date_tr
end = (datetime.datetime.today() -datetime.timedelta(days=1)).date()
dates_couples = date_pairs(date1, end, step=1)


ga_conc = ga_connect('208464364')

params = {'dimetions': [{'name': 'ga:isoweek'},
                        {'name': 'ga:date'},
                        {'name': 'ga:medium'},
                        {'name': 'ga:source'},
                        {'name': 'ga:dimension1'},
                        
                        {'name': 'ga:landingpagepath'}                        
                       ],
        'metrics':[{'expression': 'ga:totalevents'},
                  ],
        'filters':'ga:eventlabel=~^(ClCardSellMortgageClick|ClSerpSellMortgageClick)$;ga:landingpagepath=~/nedvizhimost;ga:source!~ohio8.vchecks.me'
        }

gotobro_source = ga_conc.report_pd(dates_couples,params)


params = {'dimetions': [{'name': 'ga:isoweek'},
                        {'name': 'ga:date'},
                        {'name': 'ga:medium'},
                        {'name': 'ga:source'},
                        {'name': 'ga:eventlabel'},
                        {'name': 'ga:eventaction'},
                        
                        {'name': 'ga:landingpagepath'} 
                       ],
        'metrics':[{'expression': 'ga:totalevents'},
                  ],
        'filters':'ga:eventlabel=~^claim_completed;ga:landingpagepath=~/nedvizhimost'
        }

lead_bro_source = ga_conc.report_pd(dates_couples,params)


params = {'dimetions': [{'name': 'ga:isoweek'},
                        {'name': 'ga:date'},
                        {'name': 'ga:medium'},
                        {'name': 'ga:source'},
                        {'name': 'ga:eventlabel'}
                       ],
        'metrics':[{'expression': 'ga:totalevents'},
                  ],
        'filters':'ga:eventlabel=~SendFormRegSuccess;ga:source!~ohio8.vchecks.me'
        }

reg_all = ga_conc.report_pd(dates_couples,params)

params = {'dimetions': [{'name': 'ga:isoweek'},
                        {'name': 'ga:date'},
                        {'name': 'ga:medium'},
                        {'name': 'ga:source'},
                        {'name': 'ga:eventlabel'}
                       ],
        'metrics':[{'expression': 'ga:totalevents'},
                  ],
        'filters':'ga:eventlabel=~SendFormRegSuccess;ga:landingpagepath=~/nedvizhimost;ga:source!~ohio8.vchecks.me'
        }

reg_up = ga_conc.report_pd(dates_couples,params)

gotobro_source.to_gbq(f'UA_REPORTS.MISC_Transfer_broker', project_id='m2-main', if_exists='append', credentials=gbq_credential)
lead_bro_source.to_gbq(f'UA_REPORTS.MISC_Leads_broker', project_id='m2-main', if_exists='append', credentials=gbq_credential)
reg_all.to_gbq(f'UA_REPORTS.MISC_Registration_All', project_id='m2-main', if_exists='append', credentials=gbq_credential)
reg_up.to_gbq(f'UA_REPORTS.MISC_Registration_UP', project_id='m2-main', if_exists='append', credentials=gbq_credential)

a = '''SELECT * FROM m2-main.UA_REPORTS.MISC_Transfer_broker where date >= '2022-01-03' '''
b = '''SELECT * FROM m2-main.UA_REPORTS.MISC_Leads_broker where date >= '2022-01-03' '''
c = '''SELECT * FROM m2-main.UA_REPORTS.MISC_Registration_All where date >= '2022-01-03' '''
d = '''SELECT * FROM m2-main.UA_REPORTS.MISC_Registration_UP where date >= '2022-01-03' '''


gotobro_sql = pandas_gbq.read_gbq(a, project_id='m2-main', credentials=gbq_credential)
lead_bro_sql = pandas_gbq.read_gbq(b, project_id='m2-main', credentials=gbq_credential)
reg_all = pandas_gbq.read_gbq(c, project_id='m2-main', credentials=gbq_credential)
reg_up = pandas_gbq.read_gbq(d, project_id='m2-main', credentials=gbq_credential)

gotobro = purify_df(gotobro_sql)
lead_bro = purify_df(lead_bro_sql)

gotobro['isoweek'] = gotobro['isoweek'].astype(int)
#gotobro['date'] = pd.to_datetime(gotobro['date'], format='%Y-%m-%d')
gotobro['date'] = gotobro['date'].astype(str)
gotobro['medium'] = gotobro['medium'].astype(str)
gotobro['source'] = gotobro['source'].astype(str)
gotobro['dimension1'] = gotobro['dimension1'].astype(str)
gotobro['landingpagepath'] = gotobro['landingpagepath'].astype(str)
gotobro['totalevents'] = gotobro['totalevents'].astype(int)

lead_bro['isoweek'] = lead_bro['isoweek'].astype(int)
#lead_bro['date'] = pd.to_datetime(lead_bro['date'], format='%Y-%m-%d')
lead_bro['date'] = lead_bro['date'].astype(str)
lead_bro['medium'] = lead_bro['medium'].astype(str)
lead_bro['source'] = lead_bro['source'].astype(str)
lead_bro['eventlabel'] = lead_bro['eventlabel'].astype(str)
lead_bro['eventaction'] = lead_bro['eventaction'].astype(str)
lead_bro['landingpagepath'] = lead_bro['landingpagepath'].astype(str)
lead_bro['totalevents'] = lead_bro['totalevents'].astype(int)

reg_all['isoweek'] = reg_all['isoweek'].astype(int)
#reg_all['date'] = pd.to_datetime(reg_all['date'], format='%Y-%m-%d')
reg_all['date'] = reg_all['date'].astype(str)
reg_all['medium'] = reg_all['medium'].astype(str)
reg_all['source'] = reg_all['source'].astype(str)
reg_all['eventlabel'] = reg_all['eventlabel'].astype(str)
reg_all['totalevents'] = reg_all['totalevents'].astype(int)

reg_up['isoweek'] = reg_up['isoweek'].astype(int)
#reg_up['date'] = pd.to_datetime(reg_up['date'], format='%Y-%m-%d')
reg_up['date'] = reg_up['date'].astype(str)
reg_up['medium'] = reg_up['medium'].astype(str)
reg_up['source'] = reg_up['source'].astype(str)
reg_up['eventlabel'] = reg_up['eventlabel'].astype(str)
reg_up['totalevents'] = reg_up['totalevents'].astype(int)

gb = gotobro.groupby(['isoweek','landingpagepath']).agg(totalevents=('totalevents', 'sum')).reset_index().replace('None', 'REG')
gb_unique = gotobro.groupby(['isoweek','landingpagepath']).agg(dimension1=('dimension1', 'count')).reset_index().replace('None', 'REG')
ld = lead_bro.groupby(['isoweek','landingpagepath']).agg(totalevents=('totalevents', 'sum')).reset_index().replace('None', 'REG')
rg = reg_all.groupby(['isoweek']).agg(totalevents=('totalevents', 'sum')).reset_index()
rg_up = reg_up.groupby(['isoweek']).agg(totalevents=('totalevents', 'sum')).reset_index()

all_bro = [gb.columns.values.tolist()] + gb.values.tolist()
all_bro_unique = [gb_unique.columns.values.tolist()] + gb_unique.values.tolist()
all_bro_lead = [ld.columns.values.tolist()] + ld.values.tolist()
regs_all = [rg.columns.values.tolist()] + rg.values.tolist()
regs_up = [rg_up.columns.values.tolist()] + rg_up.values.tolist()

from apiclient.discovery import build
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials
import gspread


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
g_credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)
gc = gspread.authorize(g_credentials)


sh = gc.open("1. РК Траффик 2022 Total")
wk = sh.worksheet('report_conf')

wk.update('A2', all_bro) # Переход в брокер
wk.update('E2', all_bro_unique) #  Пользователи в брокер
wk.update('H2', all_bro_lead) # Лиды ИБ
wk.update('L2', regs_all) # Регистрация Всего
wk.update('O2', regs_up) # Регистрации УП


# OWAX COST PART

def date_iso_mon(ned):
    from datetime import datetime
    res =[]
    for i in ned:
        d = Week((datetime.now().year), i).monday()
        d = pd.to_datetime(d, format='%Y-%m-%d')
        res.append(d)
    return res

a = '''SELECT extract(isoweek from date((date))) isoweek, CITY, round(sum(COSTS), 2) as COST FROM `m2-main.TEST_REPORTS.UP_MARKETING_DASHBORD`
WHERE COSTS > 0 and extract(isoweek from date((date))) > 49
group by 1,2
order by 1,2'''

c = pandas_gbq.read_gbq(a, project_id='m2-main', credentials=gbq_credential)

cost = c.copy()

cost['start_week'] = date_iso_mon(cost['isoweek'])
cols = cost.columns.tolist()
cols.insert(1, cols.pop(cols.index('start_week')))
cost = cost.reindex(columns= cols)

cost['isoweek'] = cost['isoweek'].astype(int)
cost['COST'] = cost['COST'].astype(float)
cost['CITY'] = cost['CITY'].astype(str)
# cost['start_week'] = pd.to_datetime(cost['start_week'], format='%Y-%m-%d')
cost['start_week'] = cost['start_week'].astype(str)

g = [cost.columns.values.tolist()] + cost.values.tolist()

sh = gc.open("1. РК Траффик 2020/21 Total")
wk = sh.worksheet('source_all')

wk.update('BB2', g)
