#!/usr/bin/env python
# coding: utf-8

# In[30]:


from oauth2client.service_account import ServiceAccountCredentials
import datetime
import gspread
import pandas

import pandas as pd
from google.oauth2 import service_account
from isoweek import Week

import pandas_gbq

from apiclient.discovery import build


from ga_connector import ga_connect
from sqlalchemy import create_engine

from pandas import DataFrame as pd
from pandas import DataFrame as df

from googleapiclient.discovery import build


# In[31]:


key_path = 'C:\\Users\\messnerav\\projects\\m2-main-cd9ed0b4e222.json'
gbq_credential = service_account.Credentials.from_service_account_file(key_path,)

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name('m2ru-322008-49d82df3892d.json', SCOPES)
gc = gspread.authorize(credentials)


# key_path = '/home/web_analytics/m2-main-cd9ed0b4e222.json'
# gbq_credential = service_account.Credentials.from_service_account_file(key_path,)

# SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
#              'https://spreadsheets.google.com/feeds',
#          'https://www.googleapis.com/auth/drive']

# g_credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)
# gc = gspread.authorize(g_credentials)


# # 121-124 U2

# In[32]:


q = '''

WITH tr as (
SELECT  
session_id
, user_id
FROM `m2-main.TEST_MART.SESSIONS` WHERE CAST(date AS DATE) >='2022-01-03' AND landingpage LIKE '%nedvizhimost%' AND utm_source <> '(direct)' GROUP BY 1,2
),
ev AS (
SELECT
EXTRACT(isoweek FROM CAST(dateHourMinute AS DATE)) as isoweek
, totalEvents
, dimension4
, eventlabel
, users
FROM `m2-main.UA_REPORTS.RAW_EVENTS` 
WHERE REGEXP_CONTAINS(eventlabel, r'(ClCardSellPhoneClickTop|ClSerpSellSecondVasPhoneClick|ClCardSellPhoneClickBottom|ClCardSellSecondVasPhoneClickAll|ClSerpCardSellNewDevPaidPhoneClick)')
AND CAST(dateHourMinute AS DATE) >= '2022-01-03' AND eventlabel NOT LIKE '%ClCardSellPhoneClickAll%'
)
SELECT ev.isoweek as week
, SUM(totalEvents) AS totalEvents
, COUNT(DISTINCT user_id) AS dimension1 
FROM tr RIGHT JOIN ev ON tr.session_id=ev.dimension4  WHERE user_id IS NOT NULL GROUP BY 1 ORDER BY 1

'''
# RIGHT что бы грамотно посчитать dim1

r1 = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
r1


# In[33]:


u2 = [r1.columns.values.tolist()] + r1.values.tolist()


# # A3 BB2

# In[34]:


from oauth2client.service_account import ServiceAccountCredentials
import datetime
import gspread
import pandas

import pandas as pd
from google.oauth2 import service_account
from isoweek import Week

import pandas_gbq

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
    df['cpc_type'] = df['utm_campaign'].apply(get_cpc_soures)
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df['week'] = df['date'].apply(lambda x: x.isocalendar()[1])
    df['totalEvents'] = df['totalEvents'].astype(int)
    df['uniqueEvents'] = df['uniqueEvents'].astype(int)    
    return df


# In[35]:


q = '''

with tr as (
    SELECT CAST(date AS DATE) AS date, utm_campaign, session_id 
    FROM `m2-main.TEST_MART.SESSIONS` WHERE CAST(date AS DATE) >='2022-01-03'
),

ev as (
   SELECT pagepath, eventlabel, uniqueEvents, totalEvents, dimension4
   FROM `m2-main.UA_REPORTS.RAW_EVENTS` WHERE CAST(dateHourMinute AS DATE) >= '2022-01-03' AND REGEXP_CONTAINS(eventlabel, r'^Cl.*PhoneClick.*')
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
utm_campaign,
SUM(uniqueEvents) as uniqueEvents,
SUM(totalEvents) as totalEvents

from  tr left join ev on tr.session_id=ev.dimension4 WHERE eventlabel IS NOT NULL
group by 1,2,3,4,5

'''

hlops = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
hlops


# In[36]:


pure_df = purify_df(hlops).copy()
pure_df


# In[37]:


x = pure_df.groupby(['week','city','eventlabel','cpc_type']).agg(tot_event=('totalEvents', 'sum'), u_event=('uniqueEvents', 'sum')).reset_index()
x


# In[38]:


bb2 = [x.columns.values.tolist()] + x.values.tolist()


# #  A6 A1 "VAS. Расчет по звонкам"

# In[39]:


q = '''

select 
    extract (isoweek from dateHourMinute) as isoweek,
    REGEXP_EXTRACT(eventaction, r'(.*?)_.*?') as eventaction,
    sum(totalevents) as rashlop
from `m2-main.UA_REPORTS.RAW_EVENTS`
    where 
(eventlabel = "ClCardSellSecondVasPhoneClickAll" or 
eventlabel = "ClSerpSellSecondVasPhoneClick") and
dateHourMinute >= '2022-01-03 00:00:01 UTC' and length(REGEXP_EXTRACT(eventaction, r'(.*?)_.*?')) > 0
group by 1,2
order by 1,2

'''

r = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
r


# In[40]:


a1_vas = [r.columns.values.tolist()] + r.values.tolist()


# # MISC part1 CA2\CE2\CH2\CJ2 Брокер\Лид\Регистрация 

# In[41]:


q1 = '''

with tr as (
    SELECT date, landingpage, session_id, user_id
    FROM `m2-main.TEST_MART.SESSIONS` WHERE date>='2022-01-03' AND landingpage LIKE '%nedvizhimost%' AND landingpage NOT LIKE '%ohio8.vchecks.me%' 
),

ev as (
   SELECT totalEvents, dimension4, eventlabel, pagepath
   FROM `m2-main.UA_REPORTS.RAW_EVENTS` WHERE dateHourMinute>='2022-01-03 16:26:00 UTC' AND REGEXP_CONTAINS(eventlabel, r'^(ClCardSellMortgageClick|ClSerpSellMortgageClick)')
)

select

EXTRACT (isoweek from CAST(date AS DATETIME)) AS isoweek,
(CASE
WHEN pagepath LIKE '%mosk%' THEN 'MSK'
WHEN pagepath LIKE '%sankt-%' OR pagepath LIKE '%lening%' THEN 'SPB'
ELSE 'REG'
END) as city,
SUM(totalEvents) as totalEvents,
COUNT(DISTINCT user_id) as dimension1

from  tr left join ev on session_id = dimension4 WHERE eventlabel IS NOT NULL
group by 1,2
order by 1,2

'''

q2 = '''

with tr as (
    SELECT date, landingpage, session_id 
    FROM `m2-main.TEST_MART.SESSIONS` WHERE date>='2022-01-03' AND landingpage LIKE '%nedvizhimost%' 
),

ev as (
   SELECT eventlabel, totalEvents, dimension4, pagepath
   FROM `m2-main.UA_REPORTS.RAW_EVENTS` WHERE dateHourMinute>='2022-01-03 16:26:00 UTC' AND REGEXP_CONTAINS(eventlabel, r'^claim_completed')
)

select

EXTRACT (isoweek from CAST(date AS DATETIME)) AS isoweek,
(CASE
WHEN landingpage LIKE '%mosk%' THEN 'MSK'
WHEN landingpage LIKE '%sankt-%' OR landingpage LIKE '%lening%' THEN 'SPB'
ELSE 'REG'
END) as city,
SUM(totalEvents) as totalEvents

from  tr left join ev on session_id = dimension4 WHERE eventlabel IS NOT NULL
group by 1,2
order by 1,2

'''

q3 = '''

with tr as (
    SELECT date, landingpage, session_id 
    FROM `m2-main.TEST_MART.SESSIONS` WHERE date>='2022-01-03' AND landingpage NOT LIKE '%ohio8.vchecks.me%' 
),

ev as (
   SELECT eventlabel, totalEvents, dimension4
   FROM `m2-main.UA_REPORTS.RAW_EVENTS` WHERE dateHourMinute>='2022-01-19 00:00:01 UTC' AND REGEXP_CONTAINS(eventlabel, r'(NewRegSuccess)') 
   AND REGEXP_CONTAINS(eventCategory, r'(FL|P_AGENT)') AND pagepath NOT LIKE '%link-registration%'
)

select

EXTRACT (isoweek from CAST(date AS DATETIME)) AS isoweek,
SUM(totalEvents) as totalEvents

from  tr left join ev on session_id = dimension4 WHERE eventlabel IS NOT NULL
group by 1
order by 1

'''

q4 = '''

with tr as (
    SELECT date, landingpage, session_id 
    FROM `m2-main.TEST_MART.SESSIONS` WHERE date>='2022-01-03' AND landingpage NOT LIKE '%ohio8.vchecks.me%'  AND landingpage LIKE '%nedvizhimost%'
),

ev as (
   SELECT eventlabel, totalEvents, dimension4
   FROM `m2-main.UA_REPORTS.RAW_EVENTS` WHERE dateHourMinute>='2022-01-19 00:00:01 UTC' AND REGEXP_CONTAINS(eventlabel, r'(NewRegSuccess)') 
   AND REGEXP_CONTAINS(eventCategory, r'(FL|P_AGENT)')  AND pagepath NOT LIKE '%link-registration%'
)

select

EXTRACT (isoweek from CAST(date AS DATETIME)) AS isoweek,
SUM(totalEvents) as totalEvents

from  tr left join ev on session_id = dimension4 WHERE eventlabel IS NOT NULL
group by 1
order by 1

'''


# In[42]:


r1 = pandas_gbq.read_gbq(q1, project_id='m2-main', credentials=gbq_credential)
r2 = pandas_gbq.read_gbq(q2, project_id='m2-main', credentials=gbq_credential)
r3 = pandas_gbq.read_gbq(q3, project_id='m2-main', credentials=gbq_credential)
r4 = pandas_gbq.read_gbq(q4, project_id='m2-main', credentials=gbq_credential)


# In[43]:


ca2 = [r1.columns.values.tolist()] + r1.values.tolist()
ce2 = [r2.columns.values.tolist()] + r2.values.tolist()
ch2 = [r3.columns.values.tolist()] + r3.values.tolist()
cj2 = [r4.columns.values.tolist()] + r4.values.tolist()


# # MISC part2 AF2 Гарантия сделок

# In[44]:


q1 = '''

with tr as (
    SELECT EXTRACT(isoweek from date) as isoweek, landingpage, session_id, user_id
    FROM `m2-main.TEST_MART.SESSIONS` WHERE date>='2022-01-03' AND landingpage LIKE '%nedvizhimost%' GROUP BY 1,2,3,4
),

ev as (
   SELECT EXTRACT(isoweek FROM CAST(dateHourMinute AS DATE)) as isoweek, totalEvents, dimension4, eventlabel, users
   FROM `m2-main.UA_REPORTS.RAW_EVENTS` WHERE dateHourMinute>='2022-01-03 00:00:01 UTC' AND REGEXP_CONTAINS(eventlabel, r'lk_onlinebuy_lp_header_buy_click') 
)

select

ev.isoweek,
sum(users) as od_clicks

from  tr left join ev on session_id = dimension4 AND tr.isoweek = ev.isoweek
WHERE eventlabel IS NOT NULL
group by 1
order by 1

'''

q2 = '''

with a as(
SELECT
CAST (dateHourMinute AS DATE) as date,
EXTRACT (isoweek FROM dateHourMinute) as isoweek,
dimension4
FROM `m2-main.UA_REPORTS.PAGE_VIEWS`
where REGEXP_CONTAINS(pagepath, r'services/deal')AND dateHourMinute >= '2022-01-03 00:00:01 UTC' GROUP BY 1,2,3
),
b as (
SELECT
CAST(date AS DATE) as date,
extract(isoweek from CAST(date AS DATETIME)) as isoweek,
session_id, user_id
FROM `m2-main.TEST_MART.SESSIONS`
where landingpage LIKE '%nedvizhimost%' AND CAST(date AS DATE) >='2022-01-03' GROUP BY 1,2,3,4
),
c as (
SELECT
CAST(date as DATE) as date,
EXTRACT (isoweek FROM CAST(date as DATE)) as isoweek, 
dimension2, dimension1
FROM `m2-main.UA_REPORTS.USERS`
WHERE dimension2 <> 'not_authorized' AND CAST(date as DATE) >= '2022-01-03' GROUP BY 1,2,3,4
)

SELECT
c.isoweek,
COUNT (DISTINCT c.dimension1) as od_us
from a left join b on a.dimension4=b.session_id and a.date = b.date
left join c on b.user_id =  c.dimension1 and b.date = c.date 
GROUP BY 1  HAVING od_us > 0 ORDER BY 1 

'''

q3 = '''

with tr as (
    SELECT EXTRACT(isoweek from date) as isoweek, landingpage, session_id, user_id
    FROM `m2-main.TEST_MART.SESSIONS` WHERE date>='2022-01-03' AND landingpage LIKE '%nedvizhimost%' GROUP BY 1,2,3,4
),

ev as (
   SELECT EXTRACT(isoweek FROM CAST(dateHourMinute AS DATE)) as isoweek, totalEvents, dimension4, eventlabel, users
   FROM `m2-main.UA_REPORTS.RAW_EVENTS` WHERE dateHourMinute>='2022-01-03 00:00:01 UTC' AND REGEXP_CONTAINS(eventlabel, r'gd_start') 
)

select

ev.isoweek,
sum(users) as gd_clicks

from  tr left join ev on session_id = dimension4 AND tr.isoweek = ev.isoweek
WHERE eventlabel IS NOT NULL
group by 1
order by 1

'''

q4 = '''

with a as(
SELECT
CAST (dateHourMinute AS DATE) as date,
EXTRACT (isoweek FROM dateHourMinute) as isoweek,
dimension4
FROM `m2-main.UA_REPORTS.PAGE_VIEWS`
where REGEXP_CONTAINS(pagepath, r'guaranteed-deal')AND dateHourMinute >= '2022-01-03 00:00:01 UTC' GROUP BY 1,2,3
),
b as (
SELECT
CAST(date AS DATE) as date,
extract(isoweek from CAST(date AS DATETIME)) as isoweek,
session_id, user_id
FROM `m2-main.TEST_MART.SESSIONS` 
where landingpage LIKE '%nedvizhimost%' AND CAST(date AS DATE) >='2022-01-03' GROUP BY 1,2,3,4
),
c as (
SELECT
CAST(date as DATE) as date,
EXTRACT (isoweek FROM CAST(date as DATE)) as isoweek, 
dimension2, dimension1
FROM `m2-main.UA_REPORTS.USERS`
WHERE dimension2 <> 'not_authorized' AND CAST(date as DATE) >= '2022-01-03' GROUP BY 1,2,3,4
)

SELECT
c.isoweek,
COUNT (DISTINCT c.dimension1) as gd_us
from a left join b on a.dimension4=b.session_id and a.date = b.date
left join c on b.user_id =  c.dimension1 and b.date = c.date 
GROUP BY 1  HAVING gd_us > 0 ORDER BY 1

'''


# In[45]:


r1 = pandas_gbq.read_gbq(q1, project_id='m2-main', credentials=gbq_credential)
r2 = pandas_gbq.read_gbq(q2, project_id='m2-main', credentials=gbq_credential)
r3 = pandas_gbq.read_gbq(q3, project_id='m2-main', credentials=gbq_credential)
r4 = pandas_gbq.read_gbq(q4, project_id='m2-main', credentials=gbq_credential)


# In[46]:


r1 = r1.drop(["isoweek"], axis=1).copy()
r3 = r3.drop(["isoweek"], axis=1).copy()
r4 = r4.drop(["isoweek"], axis=1).copy()


# In[47]:


frames = [r2, r4, r1, r3]
common = pd.concat(frames, axis=1)
common


# In[48]:


af2 = [common.columns.values.tolist()] + common.values.tolist()


# # WAU для планерки AV2

# In[49]:


q = '''
SELECT 
    extract(month from date(date)) as month,       
    count(distinct user_id) as uniqs

FROM `m2-main.TEST_MART.SESSIONS`

where landingpage like '%nedvizhimost%' AND utm_medium <> '(none)' AND extract(year from date(date)) = 2022

group by 1
order by 1 asc

'''

r = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)
r


# In[50]:


av2 = [r.columns.values.tolist()] + r.values.tolist()


# # B2 OWAX COSTS BO2

# In[51]:


def date_iso_mon(ned):
    from datetime import datetime
    res =[]
    for i in ned:
        d = Week((datetime.now().year), i).monday()
        d = pd.to_datetime(d, format='%Y-%m-%d')
        res.append(d)
    return res

a = '''SELECT extract(isoweek from date((date))) isoweek, CITY, round(sum(COSTS), 2) as COST FROM `m2-main.TEST_REPORTS.UP_MARKETING_DASHBORD`
WHERE COSTS > 0 and date >= '2022-01-03'
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
cost['start_week'] = cost['start_week'].astype(str)

bo2 = [cost.columns.values.tolist()] + cost.values.tolist()


# # A1 A1 "Retention УП 2022"

# In[52]:


q = '''

select 
first_dt,
year,

SUM(CASE WHEN senior =0 THEN 1 ELSE 0 END) AS New_per_week,
SUM(CASE WHEN senior =1 THEN 1 ELSE 0 END) AS week_1,
SUM(CASE WHEN senior =2 THEN 1 ELSE 0 END) AS week_2,
SUM(CASE WHEN senior =3 THEN 1 ELSE 0 END) AS week_3,
SUM(CASE WHEN senior =4 THEN 1 ELSE 0 END) AS week_4,
SUM(CASE WHEN senior =5 THEN 1 ELSE 0 END) AS week_5,
SUM(CASE WHEN senior =6 THEN 1 ELSE 0 END) AS week_6,
SUM(CASE WHEN senior =7 THEN 1 ELSE 0 END) AS week_7,
SUM(CASE WHEN senior =8 THEN 1 ELSE 0 END) AS week_8,
SUM(CASE WHEN senior =9 THEN 1 ELSE 0 END) AS week_9,
SUM(CASE WHEN senior =10 THEN 1 ELSE 0 END) AS week_10,
SUM(CASE WHEN senior =11 THEN 1 ELSE 0 END) AS week_11,
SUM(CASE WHEN senior =12 THEN 1 ELSE 0 END) AS week_12,
SUM(CASE WHEN senior =13 THEN 1 ELSE 0 END) AS week_13,
SUM(CASE WHEN senior =14 THEN 1 ELSE 0 END) AS week_14,
SUM(CASE WHEN senior =15 THEN 1 ELSE 0 END) AS week_15,
SUM(CASE WHEN senior =16 THEN 1 ELSE 0 END) AS week_16,
SUM(CASE WHEN senior =17 THEN 1 ELSE 0 END) AS week_17,
SUM(CASE WHEN senior =18 THEN 1 ELSE 0 END) AS week_18,
SUM(CASE WHEN senior =19 THEN 1 ELSE 0 END) AS week_19,
SUM(CASE WHEN senior =20 THEN 1 ELSE 0 END) AS week_20,
SUM(CASE WHEN senior =21 THEN 1 ELSE 0 END) AS week_21,
SUM(CASE WHEN senior =22 THEN 1 ELSE 0 END) AS week_22,
SUM(CASE WHEN senior =23 THEN 1 ELSE 0 END) AS week_23,
SUM(CASE WHEN senior =24 THEN 1 ELSE 0 END) AS week_24,
SUM(CASE WHEN senior =25 THEN 1 ELSE 0 END) AS week_25,
SUM(CASE WHEN senior =26 THEN 1 ELSE 0 END) AS week_26,
SUM(CASE WHEN senior =27 THEN 1 ELSE 0 END) AS week_27,
SUM(CASE WHEN senior =28 THEN 1 ELSE 0 END) AS week_28,
SUM(CASE WHEN senior =29 THEN 1 ELSE 0 END) AS week_29,
SUM(CASE WHEN senior =30 THEN 1 ELSE 0 END) AS week_30,
SUM(CASE WHEN senior =31 THEN 1 ELSE 0 END) AS week_31,
SUM(CASE WHEN senior =32 THEN 1 ELSE 0 END) AS week_32,
SUM(CASE WHEN senior =33 THEN 1 ELSE 0 END) AS week_33,
SUM(CASE WHEN senior =34 THEN 1 ELSE 0 END) AS week_34,
SUM(CASE WHEN senior =35 THEN 1 ELSE 0 END) AS week_35,
SUM(CASE WHEN senior =36 THEN 1 ELSE 0 END) AS week_36,
SUM(CASE WHEN senior =37 THEN 1 ELSE 0 END) AS week_37,
SUM(CASE WHEN senior =38 THEN 1 ELSE 0 END) AS week_38,
SUM(CASE WHEN senior =39 THEN 1 ELSE 0 END) AS week_39,
SUM(CASE WHEN senior =40 THEN 1 ELSE 0 END) AS week_40,
SUM(CASE WHEN senior =41 THEN 1 ELSE 0 END) AS week_41,
SUM(CASE WHEN senior =42 THEN 1 ELSE 0 END) AS week_42,
SUM(CASE WHEN senior =43 THEN 1 ELSE 0 END) AS week_43,
SUM(CASE WHEN senior =44 THEN 1 ELSE 0 END) AS week_44,
SUM(CASE WHEN senior =45 THEN 1 ELSE 0 END) AS week_45,
SUM(CASE WHEN senior =46 THEN 1 ELSE 0 END) AS week_46,
SUM(CASE WHEN senior =47 THEN 1 ELSE 0 END) AS week_47,
SUM(CASE WHEN senior =48 THEN 1 ELSE 0 END) AS week_48,
SUM(CASE WHEN senior =49 THEN 1 ELSE 0 END) AS week_49,
SUM(CASE WHEN senior =50 THEN 1 ELSE 0 END) AS week_50,
SUM(CASE WHEN senior =51 THEN 1 ELSE 0 END) AS week_51,
SUM(CASE WHEN senior =52 THEN 1 ELSE 0 END) AS week_52
from  (

SELECT
a.dimension1,
year,
a.iso_week,
b.first_dt as first_dt,
CASE 
WHEN a.iso_week < b.first_dt THEN ((52-b.first_dt) + a.iso_week)
ELSE a.iso_week-b.first_dt END AS senior

FROM
  
(SELECT 
extract(isoweek from CAST(date AS DATETIME)) as iso_week,
dimension1
FROM `m2-main.UA_REPORTS.UA_TRAFIC_FULL` 
where landingpagepath LIKE '%nedvizhimost%' AND CAST(date AS DATETIME) >'2021-08-09' GROUP BY 1,2) a,

(SELECT
extract(isoweek from min(CAST(date AS DATETIME))) AS first_dt,

CASE
WHEN extract(isoweek from min(CAST(date AS DATETIME))) = 52 THEN 2021
ELSE extract(year from min(CAST(date AS DATETIME))) END as year,

dimension1
FROM `m2-main.UA_REPORTS.UA_TRAFIC_FULL` where landingpagepath LIKE '%nedvizhimost%' AND CAST(date AS DATETIME) >'2021-08-09'
GROUP BY 3) b
where a.dimension1=b.dimension1 ORDER BY first_dt

) as with_week_number  group by first_dt, year order by year, first_dt;

'''

ret = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)


# In[53]:


a1_ret = [ret.columns.values.tolist()] + ret.values.tolist()


# # MISC part2  UA  Y2

# In[55]:


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


rows  = [list([get_bans.columns][0])]

for i in get_bans.itertuples():
    
    if i.isoweek == 0:
        continue
    else:
        ls=list(i)[1:]
        rows.append(ls)

y2 = rows


# In[56]:


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

g_credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)
gc = gspread.authorize(g_credentials)


sh = gc.open("1. РК Траффик 2022 Total")
wk = sh.worksheet('source_all')

wk.update('U2', u2)
wk.update('BB2', bb2)
wk.update('CA2', ca2)
wk.update('CE2', ce2)
wk.update('CH2', ch2) 
wk.update('CJ2', cj2)
wk.update('AV2', av2)
wk.update('AF2', af2)
wk.update('BO2', bo2)
wk.update('Y2', y2)

sh = gc.open("VAS. Расчет по звонкам")
wk = sh.worksheet('Расхлопы')
wk.update('A1', a1_vas)

sh = gc.open("Retention УП 2022")
wk = sh.worksheet('main')
wk.update('A1', a1_ret)


# # Расхлопы по VAS (new)

# In[57]:


q = '''
SELECT
EXTRACT (isoweek from CAST(dateHourMinute AS DATE)) as isoweek
, (CASE
WHEN pagepath LIKE '%mosk%' THEN 'MSK'
WHEN pagepath LIKE '%sankt-%' OR pagepath LIKE '%lening%' THEN 'SPB'
ELSE 'REG'
END) as city
, SUM(totalEvents) as tot
, SUM(uniqueEvents) as un
FROM `m2-main.UA_REPORTS.RAW_EVENTS` WHERE CAST(dateHourMinute AS DATE)>='2021-12-27' AND REGEXP_CONTAINS(eventlabel, r'ClCardSellSecondVasPhoneClickAll|ClSerpSellSecondVasPhoneClick')
GROUP BY 1,2
ORDER BY 1,2
'''
vas_clop = pandas_gbq.read_gbq(q, project_id='m2-main', credentials=gbq_credential)


# In[58]:


bt2 = [vas_clop.columns.values.tolist()] + vas_clop.values.tolist()


# In[59]:


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly',
             'https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

g_credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)
gc = gspread.authorize(g_credentials)


sh = gc.open("1. РК Траффик 2022 Total")
wk = sh.worksheet('source_all')

wk.update('BT2', bt2)


# In[ ]:




