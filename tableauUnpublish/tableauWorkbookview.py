import requests
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.sql import text as sa_text
import urllib

#### Connect DWH ####
server = 'tableauauto.database.windows.net'
database = 'tableauauto_db'
username = 'boon'
password = 'DEE@DA123'
driver = '{ODBC Driver 17 for SQL Server}'
dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
table = 'unpublishdashboardinwza007'

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
    engine.execute(sa_text('''TRUNCATE TABLE unpublishdashboardinwza007''').execution_options(autocommit=True))

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
    df = df[['workbook', 'owner','usage', 'id', 'name',
    'contentUrl', 'createdAt', 'updatedAt', 'viewUrlName']]

    conn = engine.connect()
    ###### Line Noti Message #####
    LineUrl = 'https://notify-api.line.me/api/notify'
    LineToken = 'XVDGomv0AlT1oztR2Ntyad7nWUYvBWU7XLHPREQYm6e'
    LineHeaders = {'Authorization':'Bearer '+ LineToken}

    try:
        dfMain = df.merge(Data, on=['owner'], how='right')
        dfMain.columns = ['workbook','owner','total_view','id','name_dashboard','contentUrl', 'createdAt', 'updatedAt', 'viewUrlName'
                        ,'domain', 'authSetting', 'email', 'externalAuthUserId', 'fullName', 'lastLogin','name', 'siteRole'
                        , 'locale', 'language']
        dfMain = dfMain.query("workbook.isnull()")
        print(dfMain)
        dfMain.to_sql(table, con=conn, if_exists = 'append', index=False, schema="dbo")
    except Exception as e:
        payload = {'message': e }
        print(payload)
        resp = requests.post(LineUrl, headers=LineHeaders , data = payload)
run()