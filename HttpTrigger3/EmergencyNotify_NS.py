from __future__ import print_function

import pickle
import pandas as pd
import os.path
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

from datetime import datetime,timedelta

import base64, requests
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import mimetypes
import os,io,shutil
import tempfile
from croniter import croniter

from apiclient import errors

from azure.storage.blob import BlobServiceClient, __version__
import pyodbc
import numpy as np
from sys import exit

### CREATE MAIL WITH ATTACHMENT ###
def create_message_with_attachment(sender, to, subject, message_text, file_list,iwidth='500'):
  core_message = MIMEMultipart('mixed')
  core_message['to'] = to
  core_message['from'] = sender
  core_message['subject'] = subject

  core_message.preamble = 'This is a multi-part message in MIME format.'
  main_message = MIMEMultipart('related')
  core_message.attach(main_message)
  message = MIMEMultipart('alternative')
  main_message.attach(message)

  txt_list = message_text.split('\n')
  html_text = ''
  for text in txt_list:
    if text.startswith('(im='):
      new_text = text.split('=')[1]
      new_text = new_text[:-1]
      html_text = html_text + '<div><img src="cid:' + new_text + '" width=' + iwidth + '><br></div>'
    else:
      html_text = html_text + '<div><p>' + text + '</p></div>'
  if html_text != '':
    msgText = MIMEText(html_text, 'html')
    message.attach(msgText)
  return {'raw': base64.urlsafe_b64encode(core_message.as_string().encode()).decode()}


def read_ggsheet(SAMPLE_SPREADSHEET_ID):
  SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

  creds = None
  
  blob_service_client = BlobServiceClient.from_connection_string('DefaultEndpointsProtocol=https;AccountName=d710rgsi01diag;AccountKey=nr/2Yn9nN9bWr0GNNSiNvBbN91MfYpkcIK0+9xcrYMdrFttcEAqV4kBBGGd8ehk+BRZ0gfe0iOTeoYVlRNbXOw==;EndpointSuffix=core.windows.net')

  container_name = 'methee-google-file'

  blob_client = blob_service_client.get_blob_client(container=container_name, blob='sheet-token.pickle')
  creds_path = os.path.join(tempfile.gettempdir(), 'sheet-token.pickle')

  with open(creds_path, "wb") as download_file:
    download_file.write(blob_client.download_blob().readall())

  with open(creds_path, "rb") as token:
    creds = pickle.load(token)

  if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
      creds.refresh(Request())
    else:
      exit
    # Save the credentials for the next run
    with open(creds_path, 'wb') as token:
      pickle.dump(creds, token)
    with open(creds_path, "rb") as data:
      blob_client.upload_blob(data,overwrite=True)

  service = build('sheets', 'v4', credentials=creds)
  return service

### SEND MAIL
def send_message(user_id, message):
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
  
  service = build('gmail', 'v1', credentials=creds)
  try:
    message = (service.users().messages().send(userId=user_id, body=message)
               .execute())
    print('Message Id: %s' % message['id'])
    return message
  except errors.HttpError:
    print('An error occurred')
    
def main(eid):
  eid = eid.upper().replace(' ','')

  #Read Employee Info
  service = read_ggsheet('1VtQGZPL78vJ8waMkyperc25LoUG5a1A-zI02QjRIIRo')
  sheet = service.spreadsheets()
  result = sheet.values().get(spreadsheetId='1VtQGZPL78vJ8waMkyperc25LoUG5a1A-zI02QjRIIRo',range='SKC+KRDA+SKL!A:O').execute()
  values = result.get('values', [])
  
  df_emp = pd.DataFrame(values)
  df_emp.columns = df_emp.iloc[0]
  df_emp.drop(df_emp.index[0], inplace=True)
  df_emp = df_emp[df_emp['เลข\nพนักงาน'].eq(eid)]
  
  if len(df_emp) == 0:
    print('eid not found')
    exit()

  #Read Employee latest answer
  server = 'p701sssi01-sd02.database.windows.net'
  database = 'UserCenter'
  username = 'dbmanager_skc'
  password = 'Bota@kubo10911'
  driver = '{ODBC Driver 17 for SQL Server}'

  mydb = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
  query = "SELECT DISTINCT top 1 *  FROM [dbo].[RegisterLocation] WHERE [eid] = '%s' order by [UpdDte_date] desc" % eid
  not_safe = pd.read_sql_query(query,mydb)
  not_safe = not_safe.merge(df_emp,left_on='eid',right_on='เลข\nพนักงาน',how='inner')
  
  #Read Mgr Info
  service = read_ggsheet('1VtQGZPL78vJ8waMkyperc25LoUG5a1A-zI02QjRIIRo')
  sheet = service.spreadsheets()
  result = sheet.values().get(spreadsheetId='1CYUFZwH4TjvAuiO_uYH3hGFLbHeUWzxQ_NL719-UFF4',range='MgrMail!A:D').execute()
  values = result.get('values', [])
  
  df_mgr = pd.DataFrame(values)
  df_mgr.columns = df_mgr.iloc[0]
  df_mgr.drop(df_mgr.index[0], inplace=True) 
  df_mgr['m_email'] = df_mgr.groupby(['m_Department','m_Division'])['m_email'].transform(lambda x: ','.join(x))
  df_mgr = df_mgr[['m_Department','m_Division','m_email']].drop_duplicates().reset_index(drop=True)
  #print(df_mgr)
  
  not_safe = not_safe.merge(df_mgr,right_on='m_Department',left_on='Department',how='inner')
  print(not_safe)

  '''#Read HR Sent Time
  service = read_ggsheet('1VtQGZPL78vJ8waMkyperc25LoUG5a1A-zI02QjRIIRo')
  sheet = service.spreadsheets()
  result = sheet.values().get(spreadsheetId='1VtQGZPL78vJ8waMkyperc25LoUG5a1A-zI02QjRIIRo',range='SentTime!A:D').execute()
  values = result.get('values', [])
  timestamp = pd.DataFrame(values)
  timestamp = pd.to_datetime(timestamp[0])[0]'''
  timestamp = datetime.today()
  
  row = not_safe.loc[0]
  to = row['m_email']
  Subject = 'รายชื่อพนักงานที่ไม่ปลอดภัย รอบวันที่ '+timestamp.strftime('%d.%m.%Y') #+' เวลา '+timestamp.strftime('%H:%M:%S')
  no = 1    
  m_from = 'skc_g.dashboard@kubota.com'
  txt = '<b>'+str(no)+'.&emsp;'+row['เลข\nพนักงาน']+'&emsp;'+row['ชื่อ  นามสกุล']+'&emsp;'+row['ชื่อตำแหน่ง']+'<b>'
            
  message = "<p>เรียน ผู้จัดการ "+row['m_Department']+"<br> <br>แจ้งรายชื่อพนักงานที่ไม่ปลอดภัย ณ วันที่ "+row['UpdDte_date'].strftime('%d.%m.%Y')+' เวลา '+row['UpdDte_date'].strftime('%H:%M:%S')+'<br>'
  message = message +'<br>'+ txt + '<br></p>'
  valid = True
  print('Send to : ',to)
  print(message)

  file_list = list()
  if valid:
      msg = create_message_with_attachment(m_from,to,Subject,message,file_list)
      send_message('me',msg)

