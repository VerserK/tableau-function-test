# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 21:06:03 2020
@author: methee.s
"""

from __future__ import print_function
import pickle
import pandas as pd
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from datetime import datetime, timedelta
from . import TableauNotiAPI as tbn
from croniter import croniter
import os
import tempfile
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from . import __init__ as urljson

def run():
    # If modifying these scopes, delete the file token.pickle.
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

    # The ID and range of a sample spreadsheet.
    SPREADSHEET_ID = '1RrGmqSDmJSlhy3wTCvUeY0gtTfQsdMe7BDgChmXDzj0'

    creds = None
  
    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string('DefaultEndpointsProtocol=https;AccountName=d710rgsi01diag;AccountKey=nr/2Yn9nN9bWr0GNNSiNvBbN91MfYpkcIK0+9xcrYMdrFttcEAqV4kBBGGd8ehk+BRZ0gfe0iOTeoYVlRNbXOw==;EndpointSuffix=core.windows.net')

    # Create a unique name for the container
    container_name = 'methee-google-file'

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob='sheet-token.pickle')
    creds_path = os.path.join(tempfile.gettempdir(), 'sheet-token.pickle')
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    with open(creds_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

    with open(creds_path, "rb") as token:
        creds = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in.
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

    # RANGE_NAME = 'LineNotify!A:F'
    test = urljson.main()
    print(test)

    # # Call the Sheets API
    # sheet = service.spreadsheets()
    # result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,range=RANGE_NAME).execute()
    # values = result.get('values', [])
    # #Convert to Pandas Dataframe
    # df = pd.DataFrame(values[1:], columns=values[0])
    # df.columns = ['DashboardName','ViewId','Token','FilterName','FilterValue','Message']
    # print(df)

    # #today = datetime.now() + timedelta(hours = 7) + timedelta(minutes=-10)
    # #base = datetime(today.year , today.month , today.day , today.hour , today.minute , 0 , 0)
    # for index, row in df.iterrows():
    #     if row['DashboardName'] != '':
    #         tbn.GetImage(row['DashboardName'],row['ViewId'],row['FilterName'],row['FilterValue'],row['Token'],row['Message'])