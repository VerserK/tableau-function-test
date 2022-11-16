from pymysql import NULL
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying , flatten_dict_column
from datetime import datetime, timezone, timedelta
import pandas as pd
import sqlalchemy as sa
import urllib
from sqlalchemy.sql import text as sa_text
import numpy as np

### Update Status
def updateSite(uid,new_site):
    config = {'tableau_online':{'server':'https://prod-apnortheast-a.online.tableau.com/',
                                'api_version':'3.17',
                                'personal_access_token_name':"TestToken",
                                "personal_access_token_secret": "5830C81FQmyACKTsgeyGjQ==:w3WLdOoielNFNbsYkcUO5ejkZruZq9x0",
                                "site_name":"skctableau",
                                "site_url":"skctableau"
            		}}
    conn = TableauServerConnection(config,env='tableau_online')
    conn.sign_in()
    
    response = conn.update_user(user_id=uid,new_site_role= new_site)
    #print(response.json())
    conn.sign_out()
    return response
def run():
    ### API Connection ###
    server = 'tableauauto.database.windows.net'
    database = 'tableauauto_db'
    username = 'boon'
    password = 'DEE@DA123'
    driver = '{ODBC Driver 17 for SQL Server}'
    dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
    table = 'allemployee'

    ### Dev01 Connection ###
    server1 = 'dwhsqldev01.database.windows.net'
    database1 = 'Tableau Data'
    username1 = 'boon'
    password1 = 'DEE@DA123'
    driver1 = '{ODBC Driver 17 for SQL Server}'
    dsn1 = 'DRIVER='+driver1+';SERVER='+server1+';PORT=1433;DATABASE='+database1+';UID='+username1+';PWD='+ password1

    params = urllib.parse.quote_plus(dsn)
    engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
    conn = engine.connect()

    params1 = urllib.parse.quote_plus(dsn1)
    engine1 = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params1)
    conn1 = engine1.connect()

    df1 = pd.read_sql("SELECT [eid],[email],[position_ID],[position_NameTH] FROM "+table, conn)

    tableau_server_config = {
            'my_env': {
                    'server': 'https://prod-apnortheast-a.online.tableau.com',
                    'api_version': '3.17',
                    'personal_access_token_name': 'TestToken',
                    'personal_access_token_secret': '5830C81FQmyACKTsgeyGjQ==:w3WLdOoielNFNbsYkcUO5ejkZruZq9x0',
                    'site_name': 'skctableau',
                    'site_url': 'skctableau'
            }
    }

    conn = TableauServerConnection(tableau_server_config, env='my_env')
    conn.sign_in()

    diff90 = datetime.now() - timedelta(days=91)
    datetoday = datetime.now()

    ### Select User On Site to SQL ###
    user_df = querying.get_users_dataframe(conn)
    user_df["lastLogin"] = pd.to_datetime(user_df["lastLogin"]).dt.strftime('%Y-%m-%d')
    user_df["lastLogin"] = pd.to_datetime(user_df["lastLogin"], format='%Y-%m-%d')
    df1['email'] = df1['email'].str.lower()
    df = user_df.merge(df1, left_on='email', right_on='email', how='left')
    df = df.drop_duplicates(subset=['email'])
    df['UpdateTime'] = pd.to_datetime(datetoday, format='%Y-%m-%d')
    df['UpdateTime'] = pd.to_datetime(df['UpdateTime'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
    engine1.execute(sa_text('''TRUNCATE TABLE tableau_alluser''').execution_options(autocommit=True))
    df.astype(str).to_sql('tableau_alluser', con=conn1, if_exists = 'append', index=False, schema="dbo")

    ### Select User overs 90 days ###
    a = df[df['lastLogin'] < diff90].copy()
    a = a.query('email.str.contains("@kubota.com")', engine='python')
    a = a.query("position_ID.notnull() and siteRole != 'Unlicensed' and position_ID != 'Div_Mgr' and position_ID != 'Dep_Mgr' and position_ID != 'VP_GM' and position_ID != 'President' and position_ID != 'Ass_Mgr' and eid.str.len() != 4 ")
    a['UpdateTime'] = pd.to_datetime(datetoday, format='%Y-%m-%d')
    a['UpdateTime'] = pd.to_datetime(a['UpdateTime'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
    print(a)
    engine1.execute(sa_text('''TRUNCATE TABLE tableau_unlicensed''').execution_options(autocommit=True))
    a.astype(str).to_sql('tableau_unlicensed', con=conn1, if_exists = 'append', index=False, schema="dbo")\

    ### Select Creator Logs ###
    CL = a.copy()
    CL = CL[CL['siteRole'] == 'Creator']
    CL.astype(str).to_sql('tableau_creator_logs', con=conn1, if_exists = 'append', index=False, schema="dbo")

    ### Change Creator to Viewer
    for index, row in a.iterrows():
        if row['siteRole'] == "Creator":
            row['siteRole'] = "Viewer"
            print(row['id'],row['email'],row['siteRole'],row['position_ID'],row['lastLogin'])
            updateSite(row['id'],row['siteRole'])

    ### Select Creator to Viewer ###
    b = a.copy()
    b = b[b['siteRole'] == "Creator"]
    b['siteRole'] = "Viewer"
    b1 = pd.read_sql("SELECT * FROM tableau_creator_toviewer", conn1)
    b2row = b[~b['email'].isin(b1['email'])]
    b2row.astype(str).to_sql('tableau_creator_toviewer', con=conn1, if_exists = 'append', index=False, schema="dbo")

    ### Select Viewer before Creator ###
    VC = b1.copy()
    VC["UpdateTime"] = pd.to_datetime(VC["UpdateTime"]).dt.strftime('%Y-%m-%d')
    VC["UpdateTime"] = pd.to_datetime(VC["UpdateTime"], format='%Y-%m-%d')
    VC = VC[VC['UpdateTime'] < diff90]
    for index, row in VC.iterrows():
        if row['siteRole'] == "Viewer":
            row['siteRole'] = "Unlicensed"
            print(row['id'],row['email'],row['siteRole'],row['position_ID'],row['lastLogin'])
            t = sa_text("DELETE FROM tableau_creator_toviewer WHERE [id]=:userid")
            updateSite(row['id'],row['siteRole'])
            engine1.execute(t, userid=row['id'])

    ### Select Viewer Logs ###
    C1rows = a[~a['email'].isin(b1['email'])]
    C1rows.astype(str).to_sql('tableau_viewer_logs', con=conn1, if_exists = 'append', index=False, schema="dbo")
    print(C1rows)
    for index, row in C1rows.iterrows():
        if row['siteRole'] == "Viewer":
            row['siteRole'] = "Unlicensed"
            print(row['id'],row['email'],row['siteRole'],row['position_ID'],row['lastLogin'])
            updateSite(row['id'],row['siteRole'])