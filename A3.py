from oauth2client.service_account import ServiceAccountCredentials
import datetime
import gspread
import pandas
import pandas as pd
from google.oauth2 import service_account
from isoweek import Week
import pandas_gbq


key_path = '/home/web_analytics/m2-main-cd9ed0b4e222.json'
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

g_credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)
gc = gspread.authorize(g_credentials)


def get_cpc_soures(cpms):
    if 'second' in cpms:
        return 'УП реклама'
    if 'new-building' in cpms:
        return 'Новосторойки реклама'
    if cpms == '(not set)':
        return 'Не рекламные'
    else:
        return 'Другие рекламные кампании'      
    
        
def purify_df(df):
    df['cpc_type'] = df['campaign'].apply(get_cpc_soures)
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df['week'] = df['date'].apply(lambda x: x.isocalendar()[1])
    df['totalEvents'] = df['totalEvents'].astype(int)
    df['uniqueEvents'] = df['uniqueEvents'].astype(int)    
    return df

q = '''
with tr as (
    SELECT date, campaign, dimension4 
    FROM `m2-main.UA_REPORTS.UA_TRAFIC_FULL` WHERE date>='2022-01-03'
),

ev as (
   SELECT pagepath, eventlabel, uniqueEvents, totalEvents, dimension4
   FROM `m2-main.UA_REPORTS.UA_ALL_CLOPS` WHERE date>='2022-01-03' AND REGEXP_CONTAINS(eventlabel, r'^Cl.*PhoneClick.*')
)

select

date,
(CASE
WHEN pagepath LIKE '%mosk%' THEN 'MSK'
WHEN pagepath LIKE '%sankt-%' OR pagepath LIKE '%lening%' THEN 'SPB'
ELSE '0'
END) as city,
pagepath,
eventlabel,
campaign,
SUM(uniqueEvents) as uniqueEvents,
SUM(totalEvents) as totalEvents

from  tr left join ev using(dimension4) WHERE eventlabel IS NOT NULL
group by 1,2,3,4,5

'''

hlops = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
pure_df = purify_df(hlops).copy()
x = pure_df.groupby(['week','city','eventlabel','cpc_type']).agg(tot_event=('totalEvents', 'sum'), u_event=('uniqueEvents', 'sum')).reset_index()
y = [x.columns.values.tolist()] + x.values.tolist()

sh = gc.open("1. РК Траффик 2022 Total")
wk = sh.worksheet('source_all')
wk.update('BB2',y)
