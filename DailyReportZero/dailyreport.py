import requests
import datetime
from pythainlp.util import thai_strftime
import sqlalchemy as sa
import urllib
import pandas as pd

def cellLoc(VIN):
    datetime_obj = datetime.datetime.now()
    datequeryStr = datetime_obj.strftime("%Y-%m-%d")

    ### Connect DB ####
    server = 'skcdwhprdmi.public.bf8966ba22c0.database.windows.net,3342'
    database =  'master'
    username = 'skcadminuser'
    password = 'DEE@skcdwhtocloud2022prd'
    driver = '{ODBC Driver 17 for SQL Server}'
    dsn = 'DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password
    params = urllib.parse.quote_plus(dsn)
    engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
    conn = engine.connect()

    query = sa.text("SELECT [LastUpdate]"
            ",[EquipmentName]"
            ",[SubDistrict]"
            ",[District]"
            ",[Province]"
            ",[Country]"
            ",[Hour]"
            ",[Rank] FROM [KIS Data].[dbo].[Engine_Location_Agg] WHERE [LastUpdate] = '" + datequeryStr + "' AND [EquipmentName] = '" + VIN + "'"
            # ",[Rank] FROM [KIS Data].[dbo].[Engine_Location_Agg] WHERE [LastUpdate] = '2023-02-13' AND [EquipmentName] = '" + VIN + "'"
            " AND [SubDistrict] <> '' AND [District] <> '' AND [Province] <> '' AND [Country] <> '' AND [Hour] > 0"
            )
    resultsetloc = conn.execute(query)
    results_as_dict_loc = resultsetloc.mappings().all()
    dfLoc = pd.DataFrame(results_as_dict_loc)
    locloop=[]
    for index, row in dfLoc.iterrows():
        address={
            "type": "text",
            "text": 'ต.'+ str(row['SubDistrict']) + ' อ.' + str(row['District']) + ' จ.' + str(row['Province']) + ' ' + str(row['Country']) + ' (' + str(row['Hour']) +' ชม.)',
            "color": "#666666",
            "size": "xs",
            "wrap": True
        }
        locloop.append(address)
    return locloop

def main():
    datetime_obj = datetime.datetime.now()
    datetimeThai = thai_strftime(datetime_obj, "%A %d %B %Y")
    datequeryStr = datetime_obj.strftime("%Y-%m-%d")

    ### Connect DB ####
    server = 'skcdwhprdmi.public.bf8966ba22c0.database.windows.net,3342'
    database =  'master'
    username = 'skcadminuser'
    password = 'DEE@skcdwhtocloud2022prd'
    driver = '{ODBC Driver 17 for SQL Server}'
    dsn = 'DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password
    params = urllib.parse.quote_plus(dsn)
    engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
    conn = engine.connect()

    ### Query Notnull ###
    query = sa.text("SELECT [LastUpdate]"
                ",[EquipmentName]"
                ",[SubDistrict]"
                ",[District]"
                ",[Province]"
                ",[Country]"
                ",[Hour]"
                ",[Rank] FROM [KIS Data].[dbo].[Engine_Location_Agg] WHERE [LastUpdate] = '" + datequeryStr + "'"
                # ",[Rank] FROM [KIS Data].[dbo].[Engine_Location_Agg] WHERE [LastUpdate] = '2023-02-13'"
                " AND [SubDistrict] <> '' AND [District] <> '' AND [Province] <> '' AND [Country] <> '' AND [Hour] > 0"
                " AND [Rank] = 1"
                )
    resultsetnotnull = conn.execute(query)
    results_as_dict_notnull = resultsetnotnull.mappings().all()
    df = pd.DataFrame(results_as_dict_notnull)

    ### Query Check User ###
    qry = sa.text("SELECT PL.[Name],PL.[TaxId],PL.[UserId],IAC.[VIN],IAC.[Product Type],IAC.[Model] FROM [Line Data].[dbo].[Profile Line] PL "
                "INNER JOIN [CRM Data].[dbo].[ID_Address_Consent] IAC ON PL.[TaxId] = IAC.[Tax ID]"
                "ORDER BY [UserId] OFFSET 0 ROWS FETCH NEXT 500 ROWS ONLY"
                )
    resultset = conn.execute(qry)
    results_as_dict = resultset.mappings().all()
    df1 = pd.DataFrame(results_as_dict)
    dfFinal = df.merge(df1, left_on='EquipmentName', right_on='VIN')
    for index, row in dfFinal.iterrows():
        url = 'https://api.line.me/v2/bot/message/push'
        headers = {'content-type': 'application/json','Authorization':'Bearer J9o+1YH2mYc/4RiFFOjgXTYqCIxT//ctqWgLjB4kyYlw8qaieSnNl42uyn/TMfk7PuWAe9S8hyL5JDIA00Vfr24Ltdq+97ds4BNk4htsAIRkiDDAVQ0PKiz2wreUTFBG4Vpv+hDtLSk1QAnu2V2pOwdB04t89/1O/w1cDnyilFU='}
        body = {
        "to": row['UserId'],
        "messages": [
            {
            "type": "flex",
            "altText": "Daily Report",
            "contents": {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "contents": [
            {
                "type": "text",
                "text": "สรุปการทำงานประจำวัน",
                "weight": "bold",
                "size": "xl"
            },
            {
                "type": "text",
                "text": datetimeThai,
                "size": "sm"
            },
            {
                "type": "text",
                "text": "เฉพาะรถที่ติด KIS เท่านั้น",
                "align": "center",
                "size": "xs"
            },
            {
                "type": "box",
                "layout": "vertical",
                "margin": "lg",
                "spacing": "sm",
                "contents": [
                {
                    "type": "box",
                    "layout": "baseline",
                    "spacing": "sm",
                    "contents": [
                    {
                        "type": "text",
                        "text": "ผลิตภัณฑ์",
                        "color": "#aaaaaa",
                        "size": "sm",
                        "flex": 3
                    },
                    {
                        "type": "text",
                        "text": row['Product Type'],
                        "wrap": True,
                        "color": "#666666",
                        "size": "sm",
                        "flex": 5
                    }
                    ]
                },
                {
                    "type": "box",
                    "layout": "baseline",
                    "spacing": "sm",
                    "contents": [
                    {
                        "type": "text",
                        "text": "หมายเลขรถ",
                        "color": "#aaaaaa",
                        "size": "sm",
                        "flex": 3
                    },
                    {
                        "type": "text",
                        "text": row['VIN'],
                        "wrap": True,
                        "color": "#666666",
                        "size": "sm",
                        "flex": 5
                    }
                    ]
                },
                {
                    "type": "box",
                    "layout": "baseline",
                    "contents": [
                    {
                        "type": "text",
                        "text": "รุ่น",
                        "color": "#aaaaaa",
                        "size": "sm",
                        "flex": 3
                    },
                    {
                        "type": "text",
                        "text": row['Model'],
                        "flex": 5,
                        "color": "#666666",
                        "size": "sm",
                        "wrap": True
                    }
                    ]
                }
                ]
            },
            {
                "type": "separator",
                "margin": "md"
            },
            {
                "type": "box",
                "layout": "baseline",
                "contents": [
                {
                    "type": "icon",
                    "url": "https://dwhwebstorage.blob.core.windows.net/test/map.png"
                },
                {
                    "type": "text",
                    "text": "สถานที่ทำงาน",
                    "size": "sm"
                }
                ],
                "margin": "md"
            },
            {
                "type": "box",
                "layout": "vertical",
                "contents": cellLoc(row['VIN'])
            }
            ]
        }
        }
            }
        ]
        }
        r = requests.post(url, headers=headers, json=body)