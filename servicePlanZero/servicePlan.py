import requests
import datetime
from pythainlp.util import thai_strftime
import sqlalchemy as sa
import urllib
import pandas as pd

def run():
    ### Connect DB ####
    server = 'skcdwhprdmi.siamkubota.co.th'
    database =  'KIS Data'
    username = 'skcadminuser'
    password = 'DEE@skcdwhtocloud2022prd'
    driver = '{ODBC Driver 17 for SQL Server}'
    dsn = 'DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password
    params = urllib.parse.quote_plus(dsn)
    engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
    conn = engine.connect()

    query = sa.text('''SELECT TOP (1000) [LastUpdate]
                ,[EquipmentName]
                ,[SubDistrict]
                ,[District]
                ,[Province]
                ,[Country]
                ,[Hour]
                ,[Rank]
            FROM [KIS Data].[dbo].[Engine_Location_Agg] WHERE LastUpdate = '2023-02-15' AND EquipmentName = 'KBCCZ494VM3F30232' AND Rank = '1'
            '''
            )
    resultsetloc = conn.execute(query)
    results_as_dict_loc = resultsetloc.mappings().all()