# -*- coding: utf-8 -*-
"""
Created on Sun Jan 30 17:56:31 2022

@author: pratipa.g
"""

import requests
import pandas as pd
from pandas import Series as Sr
from sys import exit
import sqlalchemy as sa
from sqlalchemy.sql import text as sa_text
import pandas as pd
import urllib

server = 'tableauauto.database.windows.net'
database = 'tableauauto_db'
username = 'boon'
password = 'DEE@DA123'
driver = '{ODBC Driver 17 for SQL Server}'
dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
table = 'idviewer'

def tableau_get_view_id(df,page):
    server = 'https://prod-apnortheast-a.online.tableau.com/api/3.13/'
    urlHis = server + "auth/signin"
    headers = {"Content-Type": "application/json",
               "Accept":"application/json"}
    payload = { "credentials": {
        		"personalAccessTokenName": "MailNotify",
        		"personalAccessTokenSecret": "spVlY5UoTiyqiN35jkpD5Q==:Ot21M39dqA5T8ZlpbaX9LclGsNMtdZ7q",
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
    
    url = server +  '/sites/'+site_id+'/views/?pageSize=100&pageNumber='+str(page)
    #res = requests.get(url, headers=headers, json = {})
    res = requests.get(url, headers=headers, allow_redirects=True)
    return res

def run():
    params = urllib.parse.quote_plus(dsn)
    engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
    engine.execute(sa_text('''TRUNCATE TABLE idviewer''').execution_options(autocommit=True))

    df = pd.DataFrame()
    res = tableau_get_view_id(df,1)
    resp =  res.json()

    Data = pd.DataFrame()
    resp = list(resp['views'].values())[0]

    n=2
    while True:
        Data = Data.append(pd.DataFrame(resp))
        n+=1
        res = tableau_get_view_id(df,n)
        resp =  res.json()
        try:
            resp = list(resp['views'].values())[0]
        except:
            break
    Data = Data[['workbook', 'owner', 'project', 'tags', 'location', 'id', 'name',
    'contentUrl', 'createdAt', 'updatedAt', 'viewUrlName']]

    # params = urllib.parse.quote_plus(dsn)
    # engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
    conn = engine.connect()
    #Import
    Data.astype(str).to_sql(table, con=conn, if_exists = 'append', index=False, schema="dbo")
    print("Add Sql successfully")
    # ###### Line Noti Message #####
    # LineUrl = 'https://notify-api.line.me/api/notify'
    # LineToken = 'XVDGomv0AlT1oztR2Ntyad7nWUYvBWU7XLHPREQYm6e'
    # LineHeaders = {'Authorization':'Bearer '+ LineToken}
    # payload = {'message':'Add Api Tableau to Azure SQL Server successfully'}
    # resp = requests.post(LineUrl, headers=LineHeaders , data = payload)