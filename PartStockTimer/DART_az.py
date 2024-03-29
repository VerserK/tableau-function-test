# -*- coding: utf-8 -*-
"""
Created on Tue Nov 16 11:10:06 2021

@author: pratipa.g
"""
import csv
import sys
from sys import exit
import pandas as pd
import time
import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pyodbc
import numpy as np
import os
import sqlalchemy as sa
from sqlalchemy import event
import urllib
from ftplib import FTP
import requests
import traceback
from azure.storage.blob import BlobServiceClient, __version__
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import pysftp
import tempfile

HOSTNAME = 'dwhtestsftp.blob.core.windows.net'
USERNAME = 'dwhtestsftp.boon'
PASSWORD = 'bcdhECAOY5j8QyE3QsdcUWkEloU5M2Dv'

server = r'SKCDWH01'
database = 'DART'
username = 'boon'
password = 'Boon@DA123'
driver = '{ODBC Driver 17 for SQL Server}'
        
dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password

def stamp_log(table,status):
    timestamp = str(datetime.today())
    SPREADSHEET_ID='1Ce5A91xYxhCABtwSFinfy2sIJmh_aeoKlFXvSz1ypH4'
    RANGE_NAME='Sheet1'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive.file','https://www.googleapis.com/auth/drive']

    creds = None
    
    with pysftp.Connection(host=HOSTNAME, username=USERNAME, password=PASSWORD) as sftp: 
        cred_path = os.path.join(tempfile.gettempdir(), 'google_authorized_user.json')
        sftp.get('google_authorized_user.json',cred_path)
        try:
            token_path =  os.path.join(tempfile.gettempdir(), 'token.json')
            sftp.get('token.json',token_path)
        except:
            os.remove(token_path)
    
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                   cred_path , SCOPES)
                creds = flow.run_local_server(port=0)
            with open(token_path, 'w') as token:
                token.write(creds.to_json())
            sftp.put(token_path,'token.json')

    service = build('sheets', 'v4', credentials=creds)

    service = service.spreadsheets()
   
    sheet = service.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=RANGE_NAME).execute()
    values = sheet.get('values', [])
    logs = pd.DataFrame(values[1:],columns=values[0])
    logs.index = logs['Table']
    logs.loc[table,['Table']] = table
    logs.loc[table,['Log timestamp']] = timestamp
    logs.loc[table,['Status']] = status
    logs['last success'] = np.where((logs['Table'].eq(table))&(status == 'Success'),timestamp,logs['last success'])
    logs['last success'] = np.where((logs['Table'].eq(table))&(status == 'Fail'),'',logs['last success'])
    logs = [logs.columns.to_list()] + logs.values.tolist()

    response_update = service.values().update(spreadsheetId=SPREADSHEET_ID,
                        range=RANGE_NAME,valueInputOption='USER_ENTERED',
                        body=dict(majorDimension='ROWS',
                        values=logs)).execute()
    return response_update

def func_LineNotify(Message,LineToken = 'pTfbjW6EG1oWMT7rY0N3v50dqRzg038xjSLbHXF9C4y'):
    url  = "https://notify-api.line.me/api/notify"
    msn = {'message':Message}
    LINE_HEADERS = {"Authorization":"Bearer " + LineToken}
    session  = requests.Session()
    response =session.post(url, headers=LINE_HEADERS, data=msn)
    # response = Message
    return response 

def uploadCSV(data, filepath,table,dsn):

    mydb = pyodbc.connect(dsn)
    
    cursor = mydb.cursor()
    
    df = data
    
    with pysftp.Connection(host=HOSTNAME, username=USERNAME, password=PASSWORD) as sftp: 
        col_path = os.path.join(tempfile.gettempdir(), r'PartField_to_Map.csv')
        sftp.get(r'PartField_to_Map.csv',col_path)
    
    df_cols = pd.read_csv(col_path)
    
    for col in df.columns:
        
        if table in ['PART_SALES','retail_sales']:
            if col == 'HDISC':
                df = df.drop(columns = [col])
                continue
            if col in ['FKIMG','NETPR','DCCOUPP','DLRDISCA','DLRDISCP','SKCPSCP','SKCPSCA','IDISC',
                       'NETWR','DCCOUPP_BAHT','DLRDISCP_BAHT','SKCPSCP_BAHT','MDISCPER','MDISCPER_BAHT',
                       'MDISCAMT','SKCCSLYT','SKCCSLYT_BAHT','SKCCSLYTAMT','SKCPONTWVAT','DLRPSCP','DLRPSCP_BAHT','DLRPSCA']:
                print(col)
                df[col] = df[col].fillna(0).astype(str)
                df[col] = df[col].str.replace(',','')
                df.loc[df[col].str.contains('-'), 'Sign'] = '-'
                df.loc[df[col].str.contains('-') == False, 'Sign'] = ''
                df[col] = df[col].str.replace('-', '')
                df[col] = df[col].str.lstrip()
                df[col] = df['Sign'].astype(str)+df[col].astype(str)
                df['Sign'] = ''
                df[col] = pd.to_numeric(df[col],errors='raise')
                
            elif col in ['AUDAT','FKDAT','UDATE','VALID_FROM','VALID_TO']:
                df[col] = pd.to_datetime(df[col],errors='coerce',format='%Y%m%d')
        elif table == 'PART_STOCK':
            cols_map = df_cols[df_cols['Table']=='ZPARTSTOCK']
            if col in ['LABST','KBETR','TOTPC']:
                print(col)
                df[col] = df[col].fillna(0).astype(str)
                df[col] = df[col].str.replace(',','')
                df.loc[df[col].str.contains('-'), 'Sign'] = '-'
                df.loc[df[col].str.contains('-') == False, 'Sign'] = ''
                df[col] = df[col].str.replace('-', '')
                df[col] = df[col].str.lstrip()
                df[col] = df['Sign'].astype(str)+df[col].astype(str)
                df['Sign'] = ''
                df[col] = pd.to_numeric(df[col],errors='raise')
        elif table in ['PART_SERVICE','retail_service']:
            cols_map = df_cols[df_cols['Table']=='ZPARTSERV']
            if col in ['MILEAGE','FKIMG','NETPR','IDISC','TOTAL','NETWR']:
                print(col)
                df[col] = df[col].fillna(0).astype(str)
                df[col] = df[col].str.replace(',','')
                df.loc[df[col].str.contains('-'), 'Sign'] = '-'
                df.loc[df[col].str.contains('-') == False, 'Sign'] = ''
                df[col] = df[col].str.replace('-', '')
                df[col] = df[col].str.lstrip()
                df[col] = df['Sign'].astype(str)+df[col].astype(str)
                df['Sign'] = ''
                df[col] = pd.to_numeric(df[col],errors='coerce')
                
            elif col in ['AUDAT','FKDAT','UDATE','SALEDATE']:
                df[col] = pd.to_datetime(df[col],errors='coerce',format='%Y%m%d')
            else:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].str.replace('\t','')
        if col == 'MATNR':
            df['MATNR'] = np.where((df['MATNR'].str.len() > 11)&(df['MATNR'].ne('OTHERS-SPAREPART')) & (df['MATNR'].str.startswith("SERV")==False),df['MATNR'].str[:11]+'-'+df['MATNR'].str[11:],df['MATNR'])
            print(df['MATNR'].drop_duplicates().sort_values())
        try:
            df[col] = np.where(df[col].eq('None'),np.nan,df[col])
        except Exception as e:
            print(e)
        
        if table in ['PART_SALES','retail_sales']:
            df = df.rename(columns={col:df_cols.loc[(df_cols['Table'].eq('PART_SALES'))&(df_cols['FTP FILE'].eq(col)), 'FIELD NAME'].item()})
        elif table in ['PART_SERVICE','retail_service']:
            df = df.rename(columns={col:df_cols.loc[(df_cols['Table'].eq('PART_SERVICE'))&(df_cols['FTP FILE'].eq(col)), 'FIELD NAME'].item()})
    df.drop(columns=['Sign'],inplace=True) 

    if table == 'PART_STOCK' and filepath != 'dummystock':
        df.columns = ['KADS Code','AD Name','Branch Code','Branch Name','Material No','Material Desc','Division Code','Division Name','Quantity','Unit',
                      'List Price','Currency','Total Price','Plant Code','Plant Name','Storage Location']
        df['Quantity'] = df['Quantity'].round(0)
        print(len(df))
        df['Total Price'] = np.where(df['Total Price'].eq(np.nan) & df['Quantity'].ne(np.nan) & df['List Price'].ne(np.nan), df['Quantity']*df['List Price'], df['Total Price'])
        df = df[(df['Quantity'].gt(0))|df['Quantity'].isnull()].reset_index(drop=True)
        print(len(df))
        
    print('file len:', len(df))

    if filepath != 'dummystock':
        stamp = datetime.strptime(filepath[-18:-4],'%Y%m%d%H%M%S')
        print('Stamp query time: ',stamp)
        df['Query_datetime'] = stamp
    stamp = df['Query_datetime'][0]
    if table in ['PART_SALES','retail_sales']:
        id_list = df['Billing Doc'].astype(str).to_list()
        cycle = 200
        n = len(id_list)//cycle
        if len(id_list) > cycle:
            for i in range(0,n+1):
                print(str(i) + ' from '+ str(n))
                temp_list = id_list[i*cycle:((i+1)*cycle)].copy()
                temp_id = ["'"+temp_list[0]+"'"]
                temp_id +=[",'" + ac + "'" for ac in temp_list[1:]]
                query_string = ["DELETE FROM "+table+" WHERE [Billing Doc] IN (",")"]
                query_string[1:1] = temp_id.copy()
                strq = ' '
                for q in query_string:
                    strq +=q
                cursor.execute(strq)
        else:
            temp_id = ["'"+id_list[0]+"'"]
            temp_id +=[",'" + ac + "'" for ac in id_list[1:]]
            query_string = ["DELETE FROM "+table+" WHERE [Billing Doc] IN (",")"]
            query_string[1:1] = temp_id.copy()
            strq = ' '
            for q in query_string:
                strq +=q
            cursor.execute(strq)
    elif table in ['PART_SERVICE','retail_service']:
        id_list = df['DBM Order'].astype(str).to_list()
        cycle = 200
        n = len(id_list)//cycle
        if len(id_list) > cycle:
            for i in range(0,n+1):
                print(str(i) + ' from '+ str(n))
                temp_list = id_list[i*cycle:((i+1)*cycle)].copy()
                temp_id = ["'"+temp_list[0]+"'"]
                temp_id +=[",'" + ac + "'" for ac in temp_list[1:]]
                query_string = ["DELETE FROM "+table+" WHERE [DBM Order] IN (",")"]
                query_string[1:1] = temp_id.copy()
                strq = ' '
                for q in query_string:
                    strq +=q
                cursor.execute(strq)
        else:
            temp_id = ["'"+id_list[0]+"'"]
            temp_id +=[",'" + ac + "'" for ac in id_list[1:]]
            query_string = ["DELETE FROM "+table+" WHERE [DBM Order] IN (",")"]
            query_string[1:1] = temp_id.copy()
            strq = ' '
            for q in query_string:
                strq +=q
            cursor.execute(strq)
    else:
        del_by_date = "DELETE FROM ["+table+"] WHERE CAST([Query_datetime] as date) = '"+datetime.strftime(stamp,'%Y-%m-%d')+"'"
        cursor.execute(del_by_date)
        
    mydb.commit()
    mydb.close
   
    params = urllib.parse.quote_plus(dsn)
    engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
    conn = engine.connect()
    
    cycle = 10000
    
    @event.listens_for(conn, "before_cursor_execute")
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True
    if len(df.index) >= cycle:
        for i in range(0,len(df.index)//cycle):
            print('chunk: ' + str(i))
            dftemp = df[i*cycle:(i*cycle) + cycle]
            dftemp.to_sql(table, con=conn,if_exists = 'append', index=False, schema="dbo")
        dftemp = df[cycle*(len(df.index)//cycle):]
        dftemp.to_sql(table, con=conn,if_exists = 'append', index=False, schema="dbo")

    else:
        df.to_sql(table, con=conn,if_exists = 'append', index=False, schema="dbo")
        
    print('Finish Upload ',filepath)
    if filepath != 'dummystock':
        BLOBNAME = filepath[:-4]+'.csv'
    else:
        BLOBNAME = 'ZPARTSTOCK'+datetime.strftime(stamp,'%Y%m%d%H%M%S')+'.csv'
    temp_path = os.path.join(tempfile.gettempdir(),'tempfile.csv')
    df.to_csv(temp_path,index=False)
    with pysftp.Connection(host=HOSTNAME, username=USERNAME, password=PASSWORD) as sftp:
        sftp.cwd('/his/')
        sftp.put(temp_path,BLOBNAME)

def ReadToUpload(file,encode,table,dsn):
    with pysftp.Connection(host=HOSTNAME, username=USERNAME, password=PASSWORD) as sftp: 
        txt_path = os.path.join(tempfile.gettempdir(),file)
        sftp.cwd('/download/')
        sftp.get(file,txt_path)
    
    txt = open(txt_path, "r",encoding=encode, errors='replace')
    file_string = txt.read() #.encode('utf-8')
            
    file_list = file_string.split('\n')
            
    final_rows = []
    print(len(file_list))
    
    for no in range(len(file_list)):
        row = file_list[no]
        rows = row.split('|')
        try:
            if (len(rows[0]) != 4 or len(rows[2]) != 4) and no != 0:
                last_index = len(final_rows)-1
                pre = len(final_rows[last_index])-1
                rows = final_rows[last_index] + rows
                rows[pre] = rows[pre]+rows[pre+1]
                rows.pop(pre+1)
                final_rows = final_rows[:-1]
        except Exception as e:
            if len(rows) <= 2:
                pass
            else:
                print(rows)
                exit()
                func_LineNotify(e)
                
        if rows != ['']:
            final_rows.append(rows)
           
    final_file = pd.DataFrame(final_rows[1:])

    final_file.columns = final_rows[0]
    
    with pysftp.Connection(host=HOSTNAME, username=USERNAME, password=PASSWORD) as sftp: 
        final_path = os.path.join(tempfile.gettempdir(),file[:-4]+'.csv')
        final_file.to_csv(final_path,index=False)
        sftp.cwd('/download/')
        sftp.put(final_path,file[:-4]+'.csv')
        # sftp.put(final_path,os.path.join(r'/downloads',file[:-4]+'.csv'))

    if len(final_file) >= 1:
        uploadCSV(final_file,file, table,dsn)
        if table == 'PART_SALES':
            database = 'Parts'
            table = 'retail_sales'
            new_dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
            uploadCSV(final_file,file, table,new_dsn)
        elif table == 'PART_SERVICE':
            database = 'Parts'
            table = 'retail_service'
            new_dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
            uploadCSV(final_file,file, table,new_dsn)
            
        txt.close()

# def dummystock(dsn,refdate,noti_str):
#     refdate = datetime.strptime(refdate,'%Y%m%d').date()
#     ref_date = datetime.strftime(refdate-dt.timedelta(days=1),'%Y%m%d')
#     his_fold = r'his'
#     datediff = []
#     filename = []
#     with os.scandir(os.path.join(his_fold)) as i:
#         for entry in i:
#             if entry.is_file() and entry.name.startswith('ZPARTSTOCK'+ref_date):
#                 prv_date = pd.read_csv(os.path.join(his_fold,entry.name))
#                 print('D-1 File')
#                 thatfile = entry.name
#                 datediff = []
#                 break
#             elif entry.is_file() and entry.name.startswith('ZPARTSTOCK'):
#                 if (refdate-datetime.strptime(entry.name,'ZPARTSTOCK%Y%m%d%H%M%S.csv').date()).days > 0:
#                     filename += [entry.name]
#                     datediff += [(refdate-datetime.strptime(entry.name,'ZPARTSTOCK%Y%m%d%H%M%S.csv').date()).days]
                
#     if len(datediff) > 0:
#         thatfile = filename[datediff.index(min(datediff))]
#         print('D->1 File: ',filename[datediff.index(min(datediff))])
#         prv_date = pd.read_csv(os.path.join(his_fold,filename[datediff.index(min(datediff))]))
    
#     print('Finish Select')
#     if len(prv_date) > 0:
#         prv_date['Query_datetime'] = refdate
#         uploadCSV(prv_date, 'dummystock','PART_STOCK',dsn)
#         noti_str = noti_str+'\nPART_STOCK- dummy with '+thatfile
#     return noti_str

def main(ref_date,path,noti_str):
    ftp = FTP('172.31.1.119')
    ftp.login('kadsftp_prd','bqMVwn')
    ftp.cwd(path)
    all_files= ftp.nlst()
    
    file_names = ('ZPARTSERV'+ref_date,'ZPARTSTOCK'+ref_date,'ZPARTSALES'+ref_date) #

    hail_files = [i for i in all_files if i.startswith(file_names)]
    new_reps = []
    ftp.close()
    if len(hail_files) > 0:
        for filename in hail_files:
            ftp = FTP('172.31.1.119')
            ftp.login('kadsftp_prd','bqMVwn')

            files = ftp.dir()

            ftp.cwd(path)
            
            if filename.startswith('ZPARTSTOCK') == False: # == False: # and 'Archive' in noti_str:
                 continue
            
            print("Downloading " + filename, end=" | ")
            file_path = os.path.join(tempfile.gettempdir(),filename)
            with open(file_path, "wb") as file_handle:
                ftp.retrbinary("RETR " + filename, file_handle.write)
                with pysftp.Connection(host=HOSTNAME, username=USERNAME, password=PASSWORD) as sftp:
                    sftp.cwd('/download/') 
                    sftp.put(file_path,filename)
                print(" finished")
                file = filename
            
            encode = 'tis-620'
            if file.startswith('ZPARTSERV'):
                table = 'PART_SERVICE'
                
            elif file.startswith('ZPARTSALES'):
                table = 'PART_SALES'
                  
            elif file.startswith('ZPARTSTOCK'):
                table = 'PART_STOCK' 
            
            try:
                ReadToUpload(file,encode,table,dsn)
                #os.remove(os.path.join(r'downloads',file[:-4]+'.csv'))
                with pysftp.Connection(host=HOSTNAME, username=USERNAME, password=PASSWORD) as sftp: 
                    sftp.cwd('/download/')
                    sftp.remove(file[:-4]+'.csv')
                    sftp.remove(filename)
                #os.remove(os.path.join(r'downloads',filename))
                noti_str += '\n'+table+'- uploaded'
                stamp_log(table,'Success')
                if table == 'PART_SALES':
                    stamp_log('retail_sales','Success')
                elif table == 'PART_SERVICE':
                    stamp_log('retail_service','Success')
            except Exception as e:
                noti_str += '\n'+table+'- failed'
                print(traceback.print_exc())
                stamp_log(table,'Fail')
                if table == 'PART_SALES':
                    stamp_log('retail_sales','Fail')
                elif table == 'PART_SERVICE':
                    stamp_log('retail_service','Fail')
                func_LineNotify(e)
                
            ftpReply = ftp.close()
            if noti_str.endswith('uploaded'):
                ftp = FTP('172.31.1.119')
                ftp.login('kadsftp_prd','bqMVwn')

                files = ftp.dir()

                ftp.cwd(path)
                # ftp.rename(os.path.join('/DMP/900/KADSI187/OUT',filename), os.path.join('/DMP/900/KADSI187/ARCHIVE',filename))
                ftpReply = ftp.close()
                print("-moved-")
    return noti_str
def run():
    path = "DMP/900/KADSI187/ARCHIVE"
    outpath = "DMP/900/KADSI187/OUT"
    arc_noti = ''
    today = datetime.today() -timedelta(days=3) #datetime.strptime('20220406','%Y%m%d') #
    try:
        if today <= datetime.today():
            ref_date = datetime.strftime((today-timedelta(days=1)).date(),'%Y%m%d')  
            out_date = datetime.strftime(today.date(),'%Y%m%d')
            print(ref_date)
            out_noti = main(out_date,outpath,'DART OUT:\n')
            
            out_Resp = func_LineNotify(out_noti)
            
            # if 'STOCK' not in out_noti:
            #     try:
            #         dummystock(dsn,ref_date,arc_noti)
            #     except:
            #         out_noti += '\nPART_STOCK- dummy failed'
            
            print('end:',today)
            today += timedelta(days=1)

    except Exception as errors:
        print(traceback.print_exc())
        out_Resp = func_LineNotify(errors)