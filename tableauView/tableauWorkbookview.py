import requests
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.sql import text as sa_text
import urllib
from datetime import datetime, timezone, timedelta
import json

#### Connect DWH ####
server = 'tableauauto.database.windows.net'
database = 'tableauauto_db'
username = 'boon'
password = 'DEE@DA123'
driver = '{ODBC Driver 17 for SQL Server}'
dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
table = 'viewerdashboardinwza007'

### GET VIEW ID ###
def tableau_get_view_id(page):
    server = 'https://prod-apnortheast-a.online.tableau.com/api/3.20/'
    urlHis = server + "auth/signin"
    headers = {"Content-Type": "application/json",
               "Accept":"application/json"}
    payload = { "credentials": {
        		"personalAccessTokenName": "runFile",
        		"personalAccessTokenSecret": "G5RPfWliRZi+h1vDzeORWw==:RpEqOx6kX4M4BFDhZB3NXGYp6a5pSVy2",
        		"site": {
        			"contentUrl": "skctableau"
        		}}}
    res = requests.post(urlHis, headers=headers, json = payload)
    response =  res.json()

    token = response['credentials']['token']
    site_id = response['credentials']['site']['id']
    headers = {"Content-Type": "application/json",
           "Accept":"application/json",
           "X-Tableau-Auth": token}
    
    url = server +  '/sites/'+site_id+'/views/?includeUsageStatistics=true&pageSize=100&pageNumber='+str(page)
    #res = requests.get(url, headers=headers, json = {})
    res = requests.get(url, headers=headers, allow_redirects=True)
    return res

### GET USER ID ###
def tableau_get_user_id(page):
    server = 'https://prod-apnortheast-a.online.tableau.com/api/3.20/'
    urlHis = server + "auth/signin"
    headers = {"Content-Type": "application/json",
               "Accept":"application/json"}
    payload = { "credentials": {
        		"personalAccessTokenName": "runFile",
        		"personalAccessTokenSecret": "G5RPfWliRZi+h1vDzeORWw==:RpEqOx6kX4M4BFDhZB3NXGYp6a5pSVy2",
        		"site": {
        			"contentUrl": "skctableau"
        		}}}
    res = requests.post(urlHis, headers=headers, json = payload)
    response =  res.json()

    token = response['credentials']['token']
    site_id = response['credentials']['site']['id']
    headers = {"Content-Type": "application/json",
           "Accept":"application/json",
           "X-Tableau-Auth": token}
    url = server + '/sites/'+site_id+'/users?pageSize=100&pageNumber='+str(page)

    #res = requests.get(url, headers=headers, json = {})
    res = requests.get(url, headers=headers, allow_redirects=True)
    return res
def run():
    params = urllib.parse.quote_plus(dsn)
    engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
    engine.execute(sa_text('''TRUNCATE TABLE viewerdashboardinwza007''').execution_options(autocommit=True))

    res = tableau_get_user_id(1)
    resp =  res.json()

    Data = pd.DataFrame()
    resp = list(resp['users'].values())[0]
    n=0
    dataTmp = []
    while True:
        dataTmp.append(pd.DataFrame(resp))
        n+=1
        res = tableau_get_user_id(n)
        resp =  res.json()
        try:
            resp = list(resp['users'].values())[0]
        except:
            break
    Data = pd.concat(dataTmp)
    Data['domain'] = Data.domain.dropna().apply(pd.Series)
    Data.columns = ['domain', 'authSetting', 'email', 'externalAuthUserId', 'fullName', 'owner', 'lastLogin','name', 'siteRole', 'locale', 'language']
    Data = Data.drop_duplicates(subset=['owner'])
    # Data['lastLogin'] = pd.to_datetime(df['lastLogin'], format='%d.%m.%Y')

    tmp = []
    res = tableau_get_view_id(1)
    resp =  res.json()

    resp = list(resp['views'].values())[0]

    n=0
    while True:
        tmp.append(pd.DataFrame(resp))
        n+=1
        res = tableau_get_view_id(n)
        resp =  res.json()
        try:
            resp = list(resp['views'].values())[0]
        except:
            break
    df = pd.concat(tmp)
    df['owner'] = df.owner.dropna().apply(pd.Series)
    df['workbook'] = df.workbook.dropna().apply(pd.Series)
    df['usage'] = df.usage.dropna().apply(pd.Series)
    # df['createdAt'] = pd.to_datetime(df['createdAt'], format='%d.%m.%Y')
    # df['updatedAt'] = pd.to_datetime(df['updatedAt'], format='%d.%m.%Y')
    df = df[['workbook', 'owner','usage', 'id', 'name',
    'contentUrl', 'createdAt', 'updatedAt', 'viewUrlName']]

    ### Create Date ###
    url = "https://workflow.siamkubota.co.th/api/v3/reports/5704/execute"
    headers ={"authtoken":"F621C5D5-344D-4AE8-A38B-E62A3DBFDC72"}
    response = requests.get(url,headers=headers,verify=False)
    resp = json.loads(response.text)
    df_workflow = pd.DataFrame(resp['execute']['data'])
    df_workflow['Created Time'] = df_workflow.apply(lambda row : None if pd.isnull(row['Created Time']) else datetime.fromtimestamp(row['Created Time']/1000,timezone(timedelta(hours=7))).strftime('%Y-%m-%d %H:%M:%S'), axis = 1)
    df_workflow.columns = ['Group','Requester','Category','Technician','Request ID','fullName','SubCategory','Subject','Created Time']
    df_workflow = df_workflow.query("Category == 'Tableau' & SubCategory == 'Tableau Creator' | SubCategory == 'Tableau Viewer'")
    df_workflow = df_workflow.drop_duplicates(subset=['fullName'], keep='last')
    df_workflow = df_workflow.drop(['Group','Requester','Category','Technician','Request ID','SubCategory','Subject'], axis=1)

    conn = engine.connect()
    ###### Line Noti Message #####
    LineUrl = 'https://notify-api.line.me/api/notify'
    LineToken = 'XVDGomv0AlT1oztR2Ntyad7nWUYvBWU7XLHPREQYm6e'
    LineHeaders = {'Authorization':'Bearer '+ LineToken}

    try:
        dfMain = df.merge(Data, on=['owner'], how='left')
        dfMain = dfMain.merge(df_workflow, on=['fullName'], how='left')
        dfMain.columns = ['workbook','owner','total_view','id','name_dashboard','contentUrl', 'createdAt', 'updatedAt', 'viewUrlName'
                        ,'domain', 'authSetting', 'email', 'externalAuthUserId', 'fullName', 'lastLogin','name', 'siteRole'
                        , 'locale', 'language','create_date']
        dfMain.to_sql(table, con=conn, if_exists = 'append', index=False, schema="dbo")
    except Exception as e:
        payload = {'message': e }
        resp = requests.post(LineUrl, headers=LineHeaders , data = payload)