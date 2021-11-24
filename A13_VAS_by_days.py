#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas_gbq
import requests
import datetime
import json
import pandas
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import gspread
import pandas as pd


# In[2]:


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name('/home/web_analytics/m2-main-cd9ed0b4e222.json', SCOPES)
gc = gspread.authorize(credentials)

# SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
#              'https://spreadsheets.google.com/feeds',
#          'https://www.googleapis.com/auth/drive']
# credentials = ServiceAccountCredentials.from_json_keyfile_name('m2ru-322008-49d82df3892d.json', SCOPES)
# gc = gspread.authorize(credentials)

sh = gc.open('VAS_по_дням')
wk = sh.worksheet('list')

list_of_dicts = wk.get_all_records()
source = pandas.DataFrame(list_of_dicts)


# In[3]:


source_tab = source.copy()


# In[4]:


source_tab.columns = [i.replace(' ','_') for i in source_tab.columns]
source_tab = source_tab[source_tab['timestamp']!='']

source_tab['start_date'] = source_tab['start_date'].apply(lambda x: datetime.datetime.strptime(x,"%d.%m.%Y" ).date())
source_tab['daily_earnings'] = source_tab['daily_earnings'] / 100

source_tab = source_tab[~source_tab['owner'].str.contains(r'[а-яА-Я]', na=False)]

cols = list(source_tab.columns)+['date_spend']
res = []
for i in source_tab.itertuples():
    
    for date_diff in range(i.days):
        standart_row = list(i[1:])
        date_spend = i.start_date + datetime.timedelta(days=date_diff)
        standart_row.append(date_spend)
        res.append(standart_row)
common = pandas.DataFrame(res, columns = cols)


# In[6]:


common['date_spend'] = common['date_spend'].astype(str)
common['start_date'] = common['start_date'].astype(str)


# In[7]:


common.info()


# In[103]:


key_path = '/home/web_analytics/m2-main-cd9ed0b4e222.json'
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)


# In[104]:


common.to_gbq(f'sheets.VAS_by_days', project_id='m2-main', if_exists='replace', credentials=gbq_credential)


# In[8]:


common2 = common[['region','agency', 'daily_earnings', 'date_spend']] .copy()
common2 = common2[common2['date_spend'] >= '2021-01-11'].copy()
common2


# In[9]:


aaa =  [common2.columns.values.tolist()] + common2.values.tolist()
aaa


# In[11]:


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name('/home/web_analytics/m2-main-cd9ed0b4e222.json', SCOPES)
gc = gspread.authorize(credentials)

# SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
#              'https://spreadsheets.google.com/feeds',
#          'https://www.googleapis.com/auth/drive']
# credentials = ServiceAccountCredentials.from_json_keyfile_name('m2ru-322008-49d82df3892d.json', SCOPES)
# gc = gspread.authorize(credentials)
      
sh = gc.open("1. РК Траффик 2020/21 Total")
wk = sh.worksheet('source_all')
wk.update('AV2',aaa)


# In[ ]:




