import requests
import json
import pandas as pd
import sqlalchemy as sa
import urllib
from sqlalchemy.sql import text as sa_text

def run():
    # ### DWH Connection ###
    server = '172.31.8.25'
    database = 'HR Data'
    username = 'boon'
    password = 'Boon@DA123'
    driver = '{ODBC Driver 17 for SQL Server}'
    dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
    table = 'allemployee'

    params = urllib.parse.quote_plus(dsn)
    engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
    conn = engine.connect()

    engine.execute(sa_text('''TRUNCATE TABLE allemployee''').execution_options(autocommit=True))

    url = "https://p701apsi01-la01skc.azurewebsites.net/skcapi/token"
    data = {'UserName': 'admin_skc', 'Password': 'Admin2020api*'}
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post(url, data=json.dumps(data), headers=headers)
    prog_dict = json.loads(r.text)
    auth_token=prog_dict["accessToken"]
    # print(prog_dict["accessToken"])
    hed = {'Authorization': 'Bearer ' + auth_token}
    url = 'https://p701apsi01-la01skc.azurewebsites.net/skcapi/empdate/allemployee'
    # url = 'https://p701apsi01-la01skc.azurewebsites.net/skcapi/empid/16132'
    response = requests.get(url, headers=hed)
    df1 = pd.DataFrame.from_dict(response.json())
    df1 = df1[["eid","titleTH","nameTH","lastnameTH","titleEN","nameEN","lastnameEN","email","jobNameTH","bU5ID","bU6ID","bU8ID"]]
    df1.columns = ['eid','titleTH','nameTH','lastnameTH','titleEN','nameEN','lastnameEN','email','jobNameTH','bu5id','bu6id','bu8id']

    url = 'https://p701apsi01-la01skc.azurewebsites.net/skcapi/bu5master'
    response = requests.get(url, headers=hed)
    df5 = pd.DataFrame.from_dict(response.json())
    df5 = df5[["bu5id","bu5nameTh"]]


    url = 'https://p701apsi01-la01skc.azurewebsites.net/skcapi/bu6master'
    response = requests.get(url, headers=hed)
    df6 = pd.DataFrame.from_dict(response.json())
    df6 = df6[["bu6id","bu6nameTh"]]


    url = 'https://p701apsi01-la01skc.azurewebsites.net/skcapi/bu8master'
    response = requests.get(url, headers=hed)
    df8 = pd.DataFrame.from_dict(response.json())
    df8 = df8[["bu8id","bu8nameTh"]]

    df1 = df1.set_index('bu5id').join(df5.set_index('bu5id'))

    df1 = df1.set_index('bu6id').join(df6.set_index('bu6id'))

    df1 = df1.set_index('bu8id').join(df8.set_index('bu8id'))

    df1 = df1.reset_index()
    df1.drop('bu8id', axis=1, inplace=True)
    # df1 = df1[["eid","titleTH","nameTH","lastnameTH","nameEN","lastnameEN","email","jobNameTH","bu5nameTh","bu6nameTh","bu8nameTh"]]
    df1.columns = ['eid','titleTH','nameTH','lastnameTH','titleEN','nameEN','lastnameEN','email','jobNameTH','Division','Department','Section']
    df1.to_sql(table, con=conn,if_exists = 'append', index=False, schema="dbo")
    ###### Line Noti Message #####
    LineUrl = 'https://notify-api.line.me/api/notify'
    LineToken = 'XVDGomv0AlT1oztR2Ntyad7nWUYvBWU7XLHPREQYm6e'
    LineHeaders = {'Authorization':'Bearer '+ LineToken}
    payload = {'message':'succeeds uploading SKC Employees'}
    resp = requests.post(LineUrl, headers=LineHeaders , data = payload)