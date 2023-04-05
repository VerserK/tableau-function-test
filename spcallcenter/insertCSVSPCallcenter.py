import pandas as pd
import ftplib
import urllib
import sqlalchemy as sa
from sys import exit
from sqlalchemy.sql import text as sa_text
import requests

def func_LineNotify(Message,LineToken ='XVDGomv0AlT1oztR2Ntyad7nWUYvBWU7XLHPREQYm6e'): #'qKZiexdyq6Ma5L0LH4b6kQEydQeHZF2pGG8DFHCINrs'):
    url  = "https://notify-api.line.me/api/notify"
    msn = {'message':Message}
    LINE_HEADERS = {"Authorization":"Bearer " + LineToken}
    session  = requests.Session()
    #response =session.post(url, headers=LINE_HEADERS, data=msn)
    print(Message)
    response = Message
    return response 

def run():
    #Connect DWH MI
    server = 'skcdwhprdmi.siamkubota.co.th'
    database = 'Parts'
    username = 'skcadminuser'
    password = 'DEE@skcdwhtocloud2022prd'
    driver = '{ODBC Driver 17 for SQL Server}'
    dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
    table = 'SP_CallCenter'

    params = urllib.parse.quote_plus(dsn)
    engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
    conn = engine.connect()

    conn.execute(sa_text('''TRUNCATE TABLE SP_CallCenter''').execution_options(autocommit=True))

    # Open the FTP connection
    ftp = ftplib.FTP('172.31.1.105')
    ftp.login('spdb_ftp','Kubo@5540')
    ftp.cwd('/SP_ADMIN/INTERFACE_DATA/Ticket/')

    filenames = ftp.nlst()

    for filename in filenames:

        with open( filename, 'wb' ) as file :
            ftp.retrbinary('RETR %s' % filename, file.write)

            file.close()
            aa = pd.read_csv('SP Call center.csv')
            print(aa) # Drop Column Details
    try:
        aa.to_sql(table,con=conn, if_exists = 'append', index=False, schema="dbo", chunksize=100)
    except Exception as e:
        func_LineNotify(filename+' errors','XVDGomv0AlT1oztR2Ntyad7nWUYvBWU7XLHPREQYm6e')