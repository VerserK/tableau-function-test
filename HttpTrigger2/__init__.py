import logging

import azure.functions as func
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


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    DashboardName = req.params.get('DashboardName')
    ViewId = req.params.get('ViewId')
    Token = req.params.get('Token')
    FilterName = req.params.get('FilterName')
    FilterValue = req.params.get('FilterValue')
    Time = req.params.get('Time')
    Message = req.params.get('Message')
    if not DashboardName:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            DashboardName = req_body.get('DashboardName')

    if DashboardName:
        run()
        #return func.HttpResponse(f"Hello, {DashboardName}. {ViewId} {Token} {FilterName} {FilterValue} {Time} {Message}")
        return [{DashboardName}]
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )

def run(DashboardName):
    print(f"Hello, {DashboardName}.")
    # # If modifying these scopes, delete the file token.pickle.
    # SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

    # # The ID and range of a sample spreadsheet.
    # SPREADSHEET_ID = '1RrGmqSDmJSlhy3wTCvUeY0gtTfQsdMe7BDgChmXDzj0'

    # creds = None
  
    # # Create the BlobServiceClient object which will be used to create a container client
    # blob_service_client = BlobServiceClient.from_connection_string('DefaultEndpointsProtocol=https;AccountName=d710rgsi01diag;AccountKey=nr/2Yn9nN9bWr0GNNSiNvBbN91MfYpkcIK0+9xcrYMdrFttcEAqV4kBBGGd8ehk+BRZ0gfe0iOTeoYVlRNbXOw==;EndpointSuffix=core.windows.net')

    # # Create a unique name for the container
    # container_name = 'methee-google-file'

    # # Create a blob client using the local file name as the name for the blob
    # blob_client = blob_service_client.get_blob_client(container=container_name, blob='sheet-token.pickle')
    # creds_path = os.path.join(tempfile.gettempdir(), 'sheet-token.pickle')
    # # The file token.pickle stores the user's access and refresh tokens, and is
    # # created automatically when the authorization flow completes for the first
    # # time.
    # with open(creds_path, "wb") as download_file:
    #     download_file.write(blob_client.download_blob().readall())

    # with open(creds_path, "rb") as token:
    #     creds = pickle.load(token)

    # # If there are no (valid) credentials available, let the user log in.
    # if not creds or not creds.valid:
    #     if creds and creds.expired and creds.refresh_token:
    #         creds.refresh(Request())
    #     else:
    #         exit
    #     # Save the credentials for the next run
    #     with open(creds_path, 'wb') as token:
    #         pickle.dump(creds, token)
    #     with open(creds_path, "rb") as data:
    #         blob_client.upload_blob(data,overwrite=True)

    # service = build('sheets', 'v4', credentials=creds)

    # # RANGE_NAME = 'LineNotify!A:F'
    print(test)