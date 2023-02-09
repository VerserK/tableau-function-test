from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying , flatten_dict_column
from datetime import datetime, timezone, timedelta
import pandas as pd
import sqlalchemy as sa
import urllib
from sqlalchemy.sql import text as sa_text
import requests
import numpy as np
def run():
    ### API Connection ###
    server = 'tableauauto.database.windows.net'
    database = 'tableauauto_db'
    username = 'boon'
    password = 'DEE@DA123'
    driver = '{ODBC Driver 17 for SQL Server}'
    dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
    table = 'tableau_alluser'
    params = urllib.parse.quote_plus(dsn)
    engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
    connect = engine.connect()

    tableau_server_config = {
            'my_env': {
                    'server': 'https://prod-apnortheast-a.online.tableau.com',
                    'api_version': '3.18',
                    'personal_access_token_name': 'TestToken',
                    'personal_access_token_secret': '5830C81FQmyACKTsgeyGjQ==:w3WLdOoielNFNbsYkcUO5ejkZruZq9x0',
                    'site_name': 'skctableau',
                    'site_url': 'skctableau'
            }
    }

    conn = TableauServerConnection(tableau_server_config, env='my_env')
    conn.sign_in()
    user_df = querying.get_users_dataframe(conn)
    user_df["lastLogin"] = pd.to_datetime(user_df["lastLogin"]).dt.strftime('%Y-%m-%d')
    user_df["lastLogin"] = pd.to_datetime(user_df["lastLogin"], format='%Y-%m-%d')
    user_df = user_df.query('siteRole == "Creator" or siteRole == "SiteAdministratorCreator"')
    engine.execute(sa_text('''TRUNCATE TABLE tableau_alluser''').execution_options(autocommit=True))
    user_df.astype(str).to_sql('tableau_alluser', con=connect, if_exists = 'append', index=False, schema="dbo")