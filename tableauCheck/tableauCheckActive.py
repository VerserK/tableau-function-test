from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying , flatten_dict_column
import pandas as pd
import sqlalchemy as sa
import urllib
from sqlalchemy.sql import text as sa_text
import requests
def run():
        ### Dev01 Connection ###
        server1 = 'dwhsqldev01.database.windows.net'
        database1 = 'Tableau Data'
        username1 = 'boon'
        password1 = 'DEE@DA123'
        driver1 = '{ODBC Driver 17 for SQL Server}'
        dsn1 = 'DRIVER='+driver1+';SERVER='+server1+';PORT=1433;DATABASE='+database1+';UID='+username1+';PWD='+ password1

        params1 = urllib.parse.quote_plus(dsn1)
        engine1 = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params1)
        conn1 = engine1.connect()

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
        user_df = querying.get_users_dataframe(conn)
        user_df["lastLogin"] = pd.to_datetime(user_df["lastLogin"]).dt.strftime('%Y-%m-%d')
        user_df["lastLogin"] = pd.to_datetime(user_df["lastLogin"], format='%Y-%m-%d')

        ## Email Check
        ec = pd.read_sql("SELECT * FROM tableau_83_sendmail", conn1)
        ecc = user_df[user_df['email'].isin(ec['email'])]
        ecc1 = ecc[~ecc['lastLogin'].isin(ec['lastLogin'])]

        ###### Line Noti Message #####
        LineUrl = 'https://notify-api.line.me/api/notify'
        LineToken = 'XVDGomv0AlT1oztR2Ntyad7nWUYvBWU7XLHPREQYm6e'
        LineHeaders = {'Authorization':'Bearer '+ LineToken}
        if ecc1.empty:
                print('DataFrame is empty!')
                payload = {'message' : '(Tableau Check 7 days noti) No Active Users'}
                resp = requests.post(LineUrl, headers=LineHeaders , data = payload)
                print(resp.text)
        else:
                payload=[]
                for index, row in ecc1.iterrows():
                        msg = 'Tableau is activate : '
                        payload.insert(index,row['email'])
                payload = ','.join(payload)
                msgrow = ' *Number of e-mails returned to active* : '
                numrow = ecc1['email'].count()
                converted_num = str(numrow)
                print(payload)
                resp = requests.post(LineUrl, headers=LineHeaders , data = {'message' : msgrow+converted_num})
                print(resp.text)