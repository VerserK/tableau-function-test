from __future__ import print_function

import pickle
from time import sleep
import pandas as pd
import os.path
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError
from datetime import datetime,timedelta

import base64, requests
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import os
import tempfile
from croniter import croniter
from apiclient import errors
from requests.exceptions import ReadTimeout
from azure.storage.blob import BlobServiceClient, __version__
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying , flatten_dict_column
from datetime import datetime, timezone, timedelta
import pandas as pd
import sqlalchemy as sa
import urllib
from sqlalchemy.sql import text as sa_text
import numpy as np
from email.message import EmailMessage
###Send Email
def gmail_send_message(em):
    #html contact
    html = """<!DOCTYPE html>
    <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Email</title>
    <style>
        .responsive {
        width: 100%;
        height: auto;
        }
    </style>
    </head>
    <body>
    <p>เรียน ผู้ใช้งาน Tableau</p>

    <p style="margin-left:40px">เนื่องจากระบบพบว่าท่านไม่มีการเข้าใช้งาน Tableau เป็นเวลา 83 วัน*</p>

    <p style="margin-left:40px">ระบบจะทำการ Unlicense ของท่าน <span style="color:#e74c3c">ภายใน 7 วัน</span></p>

    <p style="margin-left:40px">หากต้องการใช้งานอย่างต่อเนื่อง กรุณา <a href="https://prod-apnortheast-a.online.tableau.com/#/site/skctableau/explore">Login</a> เพื่อทำการรักษาสิทธิ์</p>

    <p style="margin-left:40px"><span style="color:#e74c3c">ในกรณี Unlicense จะถูกตัดสิทธิ์การใช้งาน หากต้องการใช้งานใหม่</span></p>

    <p style="margin-left:40px"><span style="color:#e74c3c">กรุณาเปิด Request ผ่านระบบ </span><a href="https://workflow.siamkubota.co.th/">Workflow Management System</a><span style="color:#e74c3c"> &gt; แบบฟอร์มเปิด/ปิด/แก้ไขบัญชีผู้ใช้งาน (IT0004) &gt; Tableau &gt; Tableau Creator / Tableau Viewer</span></p>

    <img src="https://dwhwebstorage.blob.core.windows.net/test/Tableau%20Online%20(2).png" alt="tableau" class="responsive">

    <p>&nbsp;</p>

    <p style="font-weight: bold; color:#e74c3c">*เงื่อนไขการตัดสิทธิ์ใช้งาน ไม่เข้าใช้งานเป็นเวลา 90 วัน</p>

    <p>จึงเรียนมาเพื่อทราบ</p>
    </body>
    </html>
    """
    creds = None
    # If modifying these scopes, delete the file token.json.
    SCOPES = ['https://www.googleapis.com/auth/gmail.compose']
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.

    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string('DefaultEndpointsProtocol=https;AccountName=d710rgsi01diag;AccountKey=nr/2Yn9nN9bWr0GNNSiNvBbN91MfYpkcIK0+9xcrYMdrFttcEAqV4kBBGGd8ehk+BRZ0gfe0iOTeoYVlRNbXOw==;EndpointSuffix=core.windows.net')

    # Create a unique name for the container
    container_name = 'methee-google-file'

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob='gmail-tableau-token.json')
    creds_path = os.path.join(tempfile.gettempdir(), 'gmail-tableau-token.json')

    with open(creds_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

    creds = Credentials.from_authorized_user_file(creds_path, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            exit
        # Save the credentials for the next run
        with open(creds_path, 'w') as token:
            token.write(creds.to_json())
        with open(creds_path, "rb") as data:
            blob_client.upload_blob(data,overwrite=True)

    try:
        service = build('gmail', 'v1', credentials=creds)
        message = EmailMessage()
        message.set_content('simple text would go here - This is a fallback for html content')
        message.add_alternative(html, subtype='html')
        message['To'] = em
        message['cc'] = ['chawannut.h@kubota.com','akarawat.p@kubota.com','chonnikan.r@kubota.com']
        message['From'] = '"SKC, Dashboard"<skc_g.dashboard@kubota.com>'
        message['Subject'] = '📍 [Tableau] ด่วน แจ้งเตือนบัญชี Tableau ของท่าน ไม่มีการเข้าใช้งานใกล้จะถึงเงื่อนไขการตัดสิทธิ์ใช้งาน'

        # encoded message
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()) \
            .decode()

        create_message = {
            'raw': encoded_message
        }
        # pylint: disable=E1101
        send_message = (service.users().messages().send
                        (userId="me", body=create_message).execute())
        print(F'Message Id: {send_message["id"]}')
    except HttpError as error:
        print(F'An error occurred: {error}')
        send_message = None
    return send_message



# em=[]
def run():
    ### API Connection ###
    server = 'tableauauto.database.windows.net'
    database = 'tableauauto_db'
    username = 'boon'
    password = 'DEE@DA123'
    driver = '{ODBC Driver 17 for SQL Server}'
    dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
    table = 'allemployee'

    params = urllib.parse.quote_plus(dsn)
    engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
    conn = engine.connect()

    df1 = pd.read_sql("SELECT [eid],[email],[position_ID],[position_NameTH] FROM "+table, conn)

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

    diff83 = datetime.now() - timedelta(days=83)
    diff30 = datetime.now() - timedelta(days=30)
    datetoday = datetime.now()

    tableau_server_config = {
            'my_env': {
                    'server': 'https://prod-apnortheast-a.online.tableau.com',
                    'api_version': '3.19',
                    'personal_access_token_name': 'TestToken',
                    'personal_access_token_secret': 'b4BQsYXjQpKr9/beZNA5XQ==:mUxmPB9adXqlsCzW3yl5VtMFSvh5cvTk',
                    'site_name': 'skctableau',
                    'site_url': 'skctableau'
            }
    }

    conn = TableauServerConnection(tableau_server_config, env='my_env')
    conn.sign_in()

    ### Select User On Site to SQL ###
    user_df = querying.get_users_dataframe(conn)
    user_df["lastLogin"] = pd.to_datetime(user_df["lastLogin"]).dt.strftime('%Y-%m-%d')
    user_df["lastLogin"] = pd.to_datetime(user_df["lastLogin"], format='%Y-%m-%d')
    df1['email'] = df1['email'].str.lower()
    df = user_df.merge(df1, left_on='email', right_on='email', how='left')
    df = df.drop_duplicates(subset=['email'])
    df['UpdateTime'] = pd.to_datetime(datetoday, format='%Y-%m-%d')
    df['UpdateTime'] = pd.to_datetime(df['UpdateTime'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')

    ### Select User overs 87 days ###
    a = df[df['lastLogin'] < diff83]
    a = a.query('email.str.contains("@kubota.com")', engine='python')
    a = a.query('''position_ID.notnull() and siteRole != 'Unlicensed' and position_ID != 'Div_Mgr' and position_ID != 'Dep_Mgr' and position_ID != 'VP_GM' and position_ID != 'President' and position_ID != 'Ass_Mgr' and position_ID != 'SEVP' and position_ID != 'VP_Dep'and eid.str.len() != 4 ''')
    a['UpdateTime'] = pd.to_datetime(datetoday, format='%Y-%m-%d')
    a['UpdateTime'] = pd.to_datetime(a['UpdateTime']).dt.date
    b1 = pd.read_sql("SELECT * FROM tableau_83_sendmail", conn1)
    b1['UpdateTime'] = pd.to_datetime(b1['UpdateTime'], format='%Y-%m-%d')
    b2 = a[~a['lastLogin'].isin(b1['lastLogin'])]
    b2.astype(str).to_sql('tableau_83_sendmail', con=conn1, if_exists = 'append', index=False, schema="dbo")
    print(b2)
    b1 = pd.read_sql("SELECT * FROM tableau_83_sendmail", conn1)
    b1['UpdateTime'] = pd.to_datetime(b1['UpdateTime'], format='%Y-%m-%d')
    b2row = b2[~b2['email'].isin(b1['email'])]
    b2row.astype(str).to_sql('tableau_83_sendmail', con=conn1, if_exists = 'append', index=False, schema="dbo")
    print(b2row)
    b3 = b2row.append(b2)
    print(b3)

    em=[]
    for index, row in b3.iterrows():
        em.append(row['email'])
    x = len(em)
    print(x)
    if x != 0:
        gmail_send_message(em)

# print(em)
# if __name__ == '__main__':
#     gmail_send_message(em)
# [END gmail_send_message]