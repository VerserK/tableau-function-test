import pandas as pd
import numpy as np
import os
import urllib
import sqlalchemy as sa
import pysftp
import datetime

server = '172.31.8.25' #172.31.8.25    
database = 'Voxtron_Callcenter' 
username = 'boon'
password = 'Boon@DA123'
driver = '{ODBC Driver 17 for SQL Server}'
dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
params = urllib.parse.quote_plus(dsn)                    
engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
conn = engine.connect()