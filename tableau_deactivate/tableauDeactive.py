from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying , flatten_dict_column
from datetime import datetime, timezone, timedelta
import pandas as pd
import sqlalchemy as sa
import urllib
from sqlalchemy.sql import text as sa_text
import numpy as np
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError
from email.message import EmailMessage
from azure.storage.blob import BlobServiceClient, __version__
import os,tempfile, base64

api_version = '3.21'
personal_access_token_name = 'NewToken'
personal_access_token_secret = 'jGkCb+1zSI2gd57CYMxtBg==:Td2CcXkoiTX5bqxk83PuJZm6cNZnH178'

###Send Email
def gmail_send_message(emailViewtoUnlicense):
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

    <p style="margin-left:40px">เนื่องจากระบบพบว่าท่านไม่มีการเข้าใช้งาน Tableau เป็นเวลา 45 วัน</p>

    <p style="margin-left:40px;color:#e74c3c">ระบบจึงทำการตัดสิทธิ์การใช้งาน (Unlicense) ของท่าน</p>

    <p style="margin-left:40px"><span style="color:#e74c3c">หากต้องการใช้งานใหม่</span></p>

    <p style="margin-left:40px"><span style="color:#e74c3c">กรุณาเปิด Request ผ่านระบบ </span><a href="https://workflow.siamkubota.co.th/">Workflow Management System</a><span style="color:#e74c3c"> &gt; แบบฟอร์มเปิด/ปิด/แก้ไขบัญชีผู้ใช้งาน (IT0004) &gt; Tableau &gt; Tableau Creator / Tableau Viewer</span></p>

    <img src="https://dwhwebstorage.blob.core.windows.net/test/How%20to%20Request%20Access%20Tableau%20and%20Data%20Warehouse%20(2).png" alt="tableau" class="responsive">

    <p>&nbsp;</p>

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
    blob_service_client = BlobServiceClient.from_connection_string('DefaultEndpointsProtocol=https;AccountName=dwhwebstorage;AccountKey=A8aP+xOBBD5ahXo9Ch6CUvzsqkM5oyGn1/R3kcFcNSrZw4aU0nE7SQCBhHQFYif1AEPlZ4/pAoP/+AStKRerPQ==;EndpointSuffix=core.windows.net')

    # Create a unique name for the container
    container_name = 'test'

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
        message['To'] = emailViewtoUnlicense
        message['cc'] = ['chawannut.h@kubota.com','akarawat.p@kubota.com','chonnikan.r@kubota.com']
        message['From'] = '"SKC, Dashboard"<skc_g.dashboard@kubota.com>'
        message['Subject'] = ' 📌[Tableau] บัญชีปัจจุบันหมดอายุ โปรดติดต่อเจ้าหน้าที่ที่เกี่ยวข้อง '

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
### Update Status
def updateSite(uid,new_site):
    config = {'tableau_online':{'server':'https://prod-apnortheast-a.online.tableau.com/',
                                'api_version': api_version,
                                'personal_access_token_name': personal_access_token_name,
                                'personal_access_token_secret': personal_access_token_secret,
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
    dsn = 'DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password
    table = 'allemployee'

    ### Dev01 Connection ###
    server1 = 'dwhsqldev01.database.windows.net'
    database1 = 'Tableau Data'
    username1 = 'boon'
    password1 = 'DEE@DA123'
    driver1 = '{ODBC Driver 17 for SQL Server}'
    dsn1 = 'DRIVER='+driver1+';SERVER='+server1+';DATABASE='+database1+';UID='+username1+';PWD='+ password1

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
                    'api_version': api_version,
                    'personal_access_token_name': personal_access_token_name,
                    'personal_access_token_secret': personal_access_token_secret,
                    'site_name': 'skctableau',
                    'site_url': 'skctableau'
            }
    }

    conn = TableauServerConnection(tableau_server_config, env='my_env')
    conn.sign_in()

    diff90 = datetime.now() - timedelta(days=46)
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
    print(df)
    engine1.execute(sa_text('''TRUNCATE TABLE tableau_alluser''').execution_options(autocommit=True))
    df.astype(str).to_sql('tableau_alluser', con=conn1, if_exists = 'append', index=False, schema="dbo")

    ### Select User overs 90 days ###
    a = df[df['lastLogin'] < diff90].copy()
    a = a.query('email.str.contains("@kubota.com")', engine='python')
    a = a.query('''position_ID.notnull() and siteRole != 'Unlicensed' and position_ID != 'Div_Mgr' and position_ID != 'Dep_Mgr' and position_ID != 'VP_GM' and position_ID != 'President' and position_ID != 'SEVP' and position_ID != 'VP_Dep' ''')
    a['UpdateTime'] = pd.to_datetime(datetoday, format='%Y-%m-%d')
    a['UpdateTime'] = pd.to_datetime(a['UpdateTime'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
    print(a)
    engine1.execute(sa_text('''TRUNCATE TABLE tableau_unlicensed''').execution_options(autocommit=True))
    a.astype(str).to_sql('tableau_unlicensed', con=conn1, if_exists = 'append', index=False, schema="dbo")\

    ### Select Creator Logs ###
    CL = a.copy()
    CL = CL[CL['siteRole'] == 'Creator']
    CL.astype(str).to_sql('tableau_creator_logs', con=conn1, if_exists = 'append', index=False, schema="dbo")
    emailCreatortoUnlicens=[]
    ### Change Creator to Viewer
    for index, row in a.iterrows():
        if row['siteRole'] == "Creator":
            row['siteRole'] = "Unlicensed"
            print(row['id'],row['email'],row['siteRole'],row['position_ID'],row['lastLogin'])
            emailCreatortoUnlicens.append(row['email'])
            updateSite(row['id'],row['siteRole'])
    x = len(emailCreatortoUnlicens)
    if x != 0:
        gmail_send_message(emailCreatortoUnlicens)

    # ### Select Creator to Viewer ###
    # b = a.copy()
    # b = b[b['siteRole'] == "Creator"]
    # b['siteRole'] = "Viewer"
    # b1 = pd.read_sql("SELECT * FROM tableau_creator_toviewer", conn1)
    # b2row = b[~b['email'].isin(b1['email'])]
    # b2row.astype(str).to_sql('tableau_creator_toviewer', con=conn1, if_exists = 'append', index=False, schema="dbo")

    # ### Select Viewer before Creator to Unlicensed and Check Update viewer to creator ###
    # VC = b1.copy()
    # VCC = b1.copy()
    # VCC = a[a['email'].isin(b1['email'])]
    # VCC = VCC[VCC['siteRole'] != "Viewer"]
    # for index, row in VCC.iterrows():
    #     print(row['id'],row['email'],row['siteRole'],row['position_ID'],row['lastLogin'])
    #     t = sa_text("DELETE FROM tableau_creator_toviewer WHERE [id]=:userid")
    #     engine1.execute(t, userid=row['id'])
    # VC["UpdateTime"] = pd.to_datetime(VC["UpdateTime"]).dt.strftime('%Y-%m-%d')
    # VC["UpdateTime"] = pd.to_datetime(VC["UpdateTime"], format='%Y-%m-%d')
    # VC = VC[VC['UpdateTime'] < diff90]
    # emailCratortoViewertoUnlicensed = []
    # for index, row in VC.iterrows():
    #     if row['siteRole'] == "Viewer":
    #         row['siteRole'] = "Unlicensed"
    #         print(row['id'],row['email'],row['siteRole'],row['position_ID'],row['lastLogin'])
    #         t = sa_text("DELETE FROM tableau_creator_toviewer WHERE [id]=:userid")
    #         emailCratortoViewertoUnlicensed.append(row['email'])
    #         updateSite(row['id'],row['siteRole'])
    #         engine1.execute(t, userid=row['id'])
    # x = len(emailCratortoViewertoUnlicensed)
    # if x != 0:
    #     gmail_send_message(emailCratortoViewertoUnlicensed)

    ### Select Viewer Logs to Unlicensed ###
    C1rows = a.copy()
    C1rows = C1rows[C1rows['siteRole'] == 'Viewer']
    C1rows.astype(str).to_sql('tableau_viewer_logs', con=conn1, if_exists = 'append', index=False, schema="dbo")
    print(C1rows)
    emailViewtoUnlicense=[]
    for index, row in C1rows.iterrows():
        if row['siteRole'] == "Viewer":
            row['siteRole'] = "Unlicensed"
            print(row['id'],row['email'],row['siteRole'],row['position_ID'],row['lastLogin'])
            emailViewtoUnlicense.append(row['email'])
            updateSite(row['id'],row['siteRole'])
    x = len(emailViewtoUnlicense)
    if x != 0:
        gmail_send_message(emailViewtoUnlicense)