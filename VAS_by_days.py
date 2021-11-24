import pandas_gbq
import requests
import datetime
import json
import pandas
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import gspread
import pandas as pd

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name('/home/web_analytics/m2-main-cd9ed0b4e222.json', SCOPES)
gc = gspread.authorize(credentials)

sh = gc.open('VAS_по_дням')
wk = sh.worksheet('list')

list_of_dicts = wk.get_all_records()
source = pandas.DataFrame(list_of_dicts)

source_tab = source.copy()

source_tab.columns = [i.replace(' ','_') for i in source_tab.columns]
source_tab = source_tab[source_tab['timestamp']!='']
source_tab['start_date'] = source_tab['start_date'].apply(lambda x: datetime.datetime.strptime(x,"%d.%m.%Y" ).date())
source_tab['daily_earnings'] = source_tab['daily_earnings'] / 100


cols = list(source_tab.columns)+['date_spend']
res = []
for i in source_tab.itertuples():
    
    for date_diff in range(i.days):
        standart_row = list(i[1:])
        date_spend = i.start_date + datetime.timedelta(days=date_diff)
        standart_row.append(date_spend)
        res.append(standart_row)
common = pandas.DataFrame(res, columns = cols)


common['date_spend'] = common['date_spend'].astype(str)
common['start_date'] = common['start_date'].astype(str)

key_path = '/home/web_analytics/m2-main-cd9ed0b4e222.json'
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)


common.to_gbq(f'sheets.VAS_by_days', project_id='m2-main', if_exists='replace', credentials=gbq_credential)


aaa =  [common.columns.values.tolist()] + common.values.tolist()

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name('/home/web_analytics/m2-main-cd9ed0b4e222.json', SCOPES)
gc = gspread.authorize(credentials)
      
sh = gc.open("1. РК Траффик 2020/21 Total")
wk = sh.worksheet('source_all')
wk.update('AV2',aaa)
