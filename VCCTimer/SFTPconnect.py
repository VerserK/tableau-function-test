import pandas as pd
import numpy as np
import os
import urllib
import sqlalchemy as sa
import pysftp
import datetime
import time

Hostname = "p701sasi09kbtcom.blob.core.windows.net"
Username = "p701sasi09kbtcom.sftpuser03"
Password = "dBIBdZipiSV+jT1i44ccs0WwYtRGoRNv"
from base64 import decodebytes
import paramiko

keydata = b"""AAAAB3NzaC1yc2EAAAADAQABAAABAQDWPK6PAGMTdzNkwKZt+A3Dhbnete6jyLLboOXWdv/QdhvjR2pNCMhGuWUxadaiLUxzZM7IvugSLGexQlZi5aCJ06DpaVYqZk/Q8l+QUydp9TfNg/kP+0OJXCJ6XdsVggboDIfrEN8ku4nfasD4QTo2tnmqZhmbIDUr38SP16PsH2bQAi2lZKg4DfWgnSFyj5sbMSDLljBEY6JQkLGiPcbqlYEN4kjB5mudE9c/ts6Jn1fhizBwJY/pE3kOydq8dCMXYFMZ6NafPacCi7Pe5zcTKfi/daioVlSXQhWK3jNzCVENonF2xWSPH+1T5F2IOV0wb0HL2l8d02x5Bw2Su4aF"""
key = paramiko.RSAKey(data=decodebytes(keydata))

cnopts =  pysftp.CnOpts()
# cnopts.hostkeys.add('aes128-gcm@openssh.com', 'rsa-sha2-256', key)
cnopts.hostkeys = None
def run():
    with pysftp.Connection(host = Hostname, username = Username, password = Password,cnopts=cnopts) as sftp:
        print("Connection successfully established ... ")
        # Switch to a remote directory
        # sftp.cwd('/dnscallcenter/')
        dnscallcenter = '/dnscallcenter/'
        archive_dir = '/archivefile/'
        today = datetime.date.today()
        today = str(today)
        # print(today)
        # Obtain structure of the remote directory '/opt'
        directory_structure = sftp.listdir_attr()

        # Print data
        for attr in directory_structure:
            if attr.filename.endswith("xlsx"):
                file = attr.filename
                fname = file.split('.')
                fname.insert(0,'VCCReport'+today)
                fname.remove('VCCReport')
                fname = '.'.join(fname)
                print(fname)
                print(file)
                sftp.rename(dnscallcenter + file, archive_dir + fname)

        print('/archivefile/VCCReport'+today+'.xlsx')
        with sftp.cd(archive_dir):
        # with sftp.open('/archivefile/VCCReport'+today+'.xlsx', 'r') as f:
            f = sftp.open('/archivefile/VCCReport'+today+'.xlsx', 'r')
            raw_df = pd.read_excel(f,header=None)

            col_name = ['Agent','Date','Time','Logon duration','Total pause duration (any)',
            'Break','Callout','Follow Up Case','Lunch','Meeting','Ronar','Toilet','Wrapup duration',
            'Ring duration total','Ring duration avg','Ring duration min','Ring duration max',
            'Idle duration total' , 'CCIN Ext','CCOUT Ext','Calls Total','Calls Serviced','Conversation duration sum',
            'Conversation duration avr']

            df_list = np.split(raw_df, raw_df[raw_df.isnull().all(1)].index)

            count = 1
            df = df_list[1]
            df = df[4:]
            df = df.reset_index(drop=True)
            df = df.dropna(how='all',axis=1)
            df.columns = col_name

            df['Datetime'] = df['Date'].astype(str).str.slice(0,10)+' '+df['Time'].astype(str)
            df.drop(columns=['Date','Time'],inplace=True)

            for c in df.columns:
                if c == 'Agent':
                    continue
                elif c == 'Datetime':
                    df[c] = pd.to_datetime(df[c].astype(str))
                elif c in ['CCIN Ext','CCOUT Ext','Calls Total','Calls Serviced']:
                    df[c] = pd.to_numeric(df[c].astype(str))
                else:
                    df[c] = pd.to_timedelta(df[c].astype(str))
                    df[c] = df[c]/ np.timedelta64(1, 'h')
                    
            df = df[['Agent', 'Datetime', 'Logon duration', 'Total pause duration (any)', 'Break',
                'Callout', 'Follow Up Case', 'Lunch', 'Meeting', 'Ronar', 'Toilet',
                'Wrapup duration', 'Ring duration total', 'Ring duration avg',
                'Ring duration min', 'Ring duration max', 'Idle duration total',
                'Calls Total', 'Calls Serviced','Conversation duration sum', 'Conversation duration avr']]

            #### optional ####
        with sftp.open('/transform/transform'+today+'.csv', 'w') as f:
            df.to_csv(f,index=False)

        server = r'SKCDWH01' #172.31.8.25    
        database = 'Voxtron_Callcenter' 
        username = 'boon'
        password = 'Boon@DA123'
        driver = '{ODBC Driver 17 for SQL Server}'
        dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
        params = urllib.parse.quote_plus(dsn)                    
        engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
        conn = engine.connect()
        df.to_sql("VCC_logs", con=conn,if_exists = 'append', index=False, schema="dbo")