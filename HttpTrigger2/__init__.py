import logging

import azure.functions as func
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

    #Convert to Pandas Dataframe
    df = pd.DataFrame({
    'DashboardName' : [DashboardName],
    'ViewId' : [ViewId],
    'Token' : [Token],
    'FilterName' : [FilterName],
    'FilterValue' : [FilterValue],
    'Message' : [Message]
    })
    df.columns = ['DashboardName','ViewId','Token','FilterName','FilterValue','Message']
    for index, row in df.iterrows():
        if row['DashboardName'] != '':
            tbn.GetImage(row['DashboardName'],row['ViewId'],row['FilterName'],row['FilterValue'],row['Token'],row['Message'])

    if DashboardName:
        # return func.HttpResponse(f"Hello, {DashboardName}. {ViewId} {Token} {FilterName} {FilterValue} {Time} {Message}")
        # return func.HttpResponse[{DashboardName}]
        return func.HttpResponse("<script>alert('Sent Line Successfully !');</script>")
    else:
        return func.HttpResponse("<script>alert('Sent Line Unsuccessful !');</script>")