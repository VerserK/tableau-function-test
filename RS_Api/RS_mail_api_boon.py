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
import logging

def tableau_get_view_id(page):
    server = 'https://prod-apnortheast-a.online.tableau.com/api/3.18/'
    urlHis = server + "auth/signin"
    headers = {"Content-Type": "application/json",
               "Accept":"application/json"}
    payload = { "credentials": {
        		"personalAccessTokenName": "1hour",
        		"personalAccessTokenSecret": "83WIskkETlmCqwnMBHsTPg==:Up2gc3VKAfb19ncBfrKY0AldNM8I1qAE",
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
    server = 'tableauauto.database.windows.net'
    database = 'tableauauto_db'
    username = 'boon'
    password = 'DEE@DA123'
    driver = '{ODBC Driver 17 for SQL Server}'
    dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
    table = 'idviewer'

    params = urllib.parse.quote_plus(dsn)
    engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
    engine.execute(sa_text('''TRUNCATE TABLE idviewer''').execution_options(autocommit=True))

    res = tableau_get_view_id(1)
    resp =  res.json()
    resp = list(resp['views'].values())[0]
    tmp = []
    n=0
    while True:
        # Data = Data.append(pd.DataFrame(resp))
        tmp.append(pd.DataFrame(resp))
        n+=1
        res = tableau_get_view_id(n)
        resp =  res.json()
        try:
            resp = list(resp['views'].values())[0]
        except:
            break
        logging.info(n)
    
    conn = engine.connect()
    ###### Line Noti Message #####
    LineUrl = 'https://notify-api.line.me/api/notify'
    LineToken = 'pTfbjW6EG1oWMT7rY0N3v50dqRzg038xjSLbHXF9C4y'
    LineHeaders = {'Authorization':'Bearer '+ LineToken}
    #Import
    try:
        df = pd.concat(tmp)
        df['owner'] = df.owner.dropna().apply(pd.Series)
        # for index,row in df.iterrows():
        #     row['owner'] = list(row['owner'].values())
        #     row['owner'] = ' '.join(row['owner'])
        logging.info(df)
        df.astype(str).to_sql(table, con=conn, if_exists = 'append', index=False, schema="dbo")
    finally:
    # Close the connection when done
        conn.close()
    # except Exception as e:
    #     logging.info(e)
    #     payload = {'message':'RS API Uploading Fails!!'}
    #     resp = requests.post(LineUrl, headers=LineHeaders , data = payload)