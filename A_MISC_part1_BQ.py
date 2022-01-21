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


q1 = '''

with tr as (
    SELECT date, landingpage, session_id, user_id
    FROM `m2-main.TEST_MART.SESSIONS` WHERE date>='2022-01-03' AND landingpage LIKE '%nedvizhimost%' AND landingpage NOT LIKE '%ohio8.vchecks.me%' 
),

ev as (
   SELECT totalEvents, dimension4, eventlabel
   FROM `m2-main.UA_REPORTS.RAW_EVENTS` WHERE dateHourMinute>='2022-01-03 16:26:00 UTC' AND REGEXP_CONTAINS(eventlabel, r'^(ClCardSellMortgageClick|ClSerpSellMortgageClick)')
)

select

EXTRACT (isoweek from CAST(date AS DATETIME)) AS isoweek,
--date,
(CASE
WHEN landingpage LIKE '%mosk%' THEN 'MSK'
WHEN landingpage LIKE '%sankt-%' OR landingpage LIKE '%lening%' THEN 'SPB'
ELSE 'REG'
END) as city,
--eventlabel,
SUM(totalEvents) as totalEvents,
COUNT(DISTINCT user_id) as dimension1

from  tr left join ev on session_id = dimension4 WHERE eventlabel IS NOT NULL
group by 1,2
order by 1,2

'''
r1 = pandas_gbq.read_gbq(q1, project_id='m2-main', credentials=gbq_credential)

q2 = '''

with tr as (
    SELECT date, landingpage, session_id 
    FROM `m2-main.TEST_MART.SESSIONS` WHERE date>='2022-01-03' AND landingpage LIKE '%nedvizhimost%' 
),

ev as (
   SELECT eventlabel, totalEvents, dimension4
   FROM `m2-main.UA_REPORTS.RAW_EVENTS` WHERE dateHourMinute>='2022-01-03 16:26:00 UTC' AND REGEXP_CONTAINS(eventlabel, r'^claim_completed')
)

select

EXTRACT (isoweek from CAST(date AS DATETIME)) AS isoweek,
--date,
(CASE
WHEN landingpage LIKE '%mosk%' THEN 'MSK'
WHEN landingpage LIKE '%sankt-%' OR landingpage LIKE '%lening%' THEN 'SPB'
ELSE 'REG'
END) as city,
--eventlabel,
SUM(totalEvents) as totalEvents

from  tr left join ev on session_id = dimension4 WHERE eventlabel IS NOT NULL
group by 1,2
order by 1,2

'''

r2 = pandas_gbq.read_gbq(q2, project_id='m2-main', credentials=gbq_credential)

q3 = '''

with tr as (
    SELECT date, landingpage, session_id 
    FROM `m2-main.TEST_MART.SESSIONS` WHERE date>='2022-01-03' AND landingpage NOT LIKE '%ohio8.vchecks.me%' 
),

ev as (
   SELECT eventlabel, totalEvents, dimension4
   FROM `m2-main.UA_REPORTS.RAW_EVENTS` WHERE dateHourMinute>='2022-01-19 00:00:01 UTC' AND REGEXP_CONTAINS(eventlabel, r'(NewRegSuccess)') 
   AND REGEXP_CONTAINS(eventCategory, r'(FL|P_AGENT)')
)

select

EXTRACT (isoweek from CAST(date AS DATETIME)) AS isoweek,
--date,
--eventlabel,
SUM(totalEvents) as totalEvents

from  tr left join ev on session_id = dimension4 WHERE eventlabel IS NOT NULL
group by 1
order by 1

'''

r3 = pandas_gbq.read_gbq(q3, project_id='m2-main', credentials=gbq_credential)

q4 = '''

with tr as (
    SELECT date, landingpage, session_id 
    FROM `m2-main.TEST_MART.SESSIONS` WHERE date>='2022-01-03' AND landingpage NOT LIKE '%ohio8.vchecks.me%'  AND landingpage LIKE '%nedvizhimost%'
),

ev as (
   SELECT eventlabel, totalEvents, dimension4
   FROM `m2-main.UA_REPORTS.RAW_EVENTS` WHERE dateHourMinute>='2022-01-19 00:00:01 UTC' AND REGEXP_CONTAINS(eventlabel, r'(NewRegSuccess)') 
   AND REGEXP_CONTAINS(eventCategory, r'(FL|P_AGENT)')
)

select

EXTRACT (isoweek from CAST(date AS DATETIME)) AS isoweek,
--date,
--eventlabel,
SUM(totalEvents) as totalEvents

from  tr left join ev on session_id = dimension4 WHERE eventlabel IS NOT NULL
group by 1
order by 1

'''

r4 = pandas_gbq.read_gbq(q4, project_id='m2-main', credentials=gbq_credential)

all_bro = [r1.columns.values.tolist()] + r1.values.tolist()
all_bro_lead = [r2.columns.values.tolist()] + r2.values.tolist()
regs_all = [r3.columns.values.tolist()] + r3.values.tolist()
regs_up = [r4.columns.values.tolist()] + r4.values.tolist()


g_credentials = ServiceAccountCredentials.from_json_keyfile_name(key_path, SCOPES)
gc = gspread.authorize(g_credentials)


sh = gc.open("1. РК Траффик 2022 Total")
wk = sh.worksheet('source_all')


wk.update('CA2', all_bro) # Переход в брокер
wk.update('CE2', all_bro_lead) # Лиды ИБ
wk.update('CH2', regs_all) # Регистрация Всего
wk.update('CJ2', regs_up) # Регистрации УП


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
# cost['start_week'] = pd.to_datetime(cost['start_week'], format='%Y-%m-%d')
cost['start_week'] = cost['start_week'].astype(str)

g = [cost.columns.values.tolist()] + cost.values.tolist()

sh = gc.open("1. РК Траффик 2022 Total")
wk = sh.worksheet('source_all')

wk.update('BO2', g)
