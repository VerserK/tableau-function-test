import pandas as pd
from pandas.tseries.offsets import MonthEnd
import requests
from bs4 import BeautifulSoup
import sqlalchemy as sa
from sqlalchemy import create_engine, MetaData, select,Table
from sqlalchemy.sql import text as sa_text
import urllib
from datetime import datetime,timedelta
from dateutil.relativedelta import *

def run():
    #config api
    url = "https://kads2-qas.siamkubotadealer.com/sap/opu/odata/SAP/ZDP_GWSRV017_SRV/$metadata"
    username = 'skc_it_skc'
    password = 'Kubota@12345'
    r = requests.get(url, auth=(username, password))
    cookieDict = r.cookies.get_dict()
    r = requests.get(url, auth=(username, password))
    cookieDict = r.cookies.get_dict()

    #configure sql server
    server = 'consentdb.database.windows.net'
    database =  'consents_QA'
    username = 'consent-user'
    password = 'P@ssc0de123'
    driver = '{ODBC Driver 17 for SQL Server}'
    dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password

    #Call Query Sql server Database Dealers
    params = urllib.parse.quote_plus(dsn)
    engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
    connection = engine.connect()
    metadata = sa.MetaData()
    dealers = sa.Table('dealers', metadata, autoload=True, autoload_with=engine)
    query = sa.select([dealers])
    ResultProxy = connection.execute(query)
    ResultSet = ResultProxy.fetchall()

    #Call Qyery Sql Server Database ActFromKADS
    paramsAct = urllib.parse.quote_plus(dsn)
    engineAct = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
    connectionAct = engineAct.connect()
    metadataAct = sa.MetaData()
    ActFromKADS = sa.Table('ActFromKADS', metadata, autoload=True, autoload_with=engine)
    queryAct = sa.select([ActFromKADS])
    ResultProxyAct = connectionAct.execute(queryAct)
    ResultSetAct = ResultProxyAct.fetchall()

    #SQL Query to Dataframe
    df = pd.DataFrame(ResultSet)
    df2 = pd.DataFrame(df['saleOrgKADS'])

    #Set time -1
    today = datetime.today() - timedelta(days=1)
    todatStr = today.strftime("%Y%m")

    #call Loop Dealer Code
    for index,row in df2.iterrows():
        url1 = "https://kads2-qas.siamkubotadealer.com/sap/opu/odata/sap/ZDP_GWSRV017_SRV/SactHdrSet?$filter=SalesOrgCode eq '"+row['saleOrgKADS']+"' and Period eq '" + todatStr + "'"
        # url1 = "https://kads2-dev.siamkubotadealer.com/sap/opu/odata/sap/ZDP_GWSRV017_SRV/SactHdrSet?$filter=SalesOrgCode eq '0120' and Period eq '" + todatStr + "'"
        print(url1)
        r1 = requests.get(url1, cookies=cookieDict)
        print(r1)
        bs_data = BeautifulSoup(r1.text, 'xml')
        bs_name = bs_data.find_all('content', {'type':'application/xml'})

        l = []
        for content in bs_name:
            ACTNOif = content.find('d:ACTNO')
            rowCheck = [content.text for content in ACTNOif]
            if rowCheck != []:
                SalesOrgCode = content.find('d:SalesOrgCode')
                row = [content.text for content in SalesOrgCode]
                l.append(row)
                Period = content.find('d:Period')
                row = [content.text for content in Period]
                l.append(row)
                ACTNO = content.find('d:ACTNO')
                row = [content.text for content in ACTNO]
                l.append(row)
                ActName = content.find('d:ActName')
                if not ActName:
                    ActName = ''
                row = [content.text for content in ActName]
                l.append(row)
                ActCode = content.find('d:ActCode')
                row = [content.text for content in ActCode]
                l.append(row)
                ActCodeTxt = content.find('d:ActCodeTxt')
                row = [content.text for content in ActCodeTxt]
                l.append(row)
                StartDate = content.find('d:StartDate')
                row = [content.text for content in StartDate]
                l.append(row)
                EndDate = content.find('d:EndDate')
                row = [content.text for content in EndDate]
                l.append(row)
                ActSts = content.find('d:ActSts')
                row = [content.text for content in ActSts]
                l.append(row)
        list=[]
        for i in l:
            for n in i:
                list.append(n)

        def to_matrix(list, n):
            return [list[i:i+n] for i in range(0, len(list), n)]

        # print(to_matrix(list,9))
        data = pd.DataFrame(to_matrix(list,9), columns=['SalesOrgCode','Period','ACTNO','ActName','ActCode','ActCodeTxt','StartDate','EndDate','ActSts'])
        data['LastUpdate'] = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
        dfAct = pd.DataFrame(ResultSetAct)
        data = data.reset_index(drop=True)
        dfAct = dfAct.reset_index(drop=True)
        if dfAct.empty:
            print('DataFrame is empty!')
            year = data['Period'].str.slice(start=0,stop=4)
            month = data['Period'].str.slice(start=4,stop=6)
            data['exp_date'] = pd.to_datetime((year+"-"+month), format='%Y-%m') + MonthEnd(1) + timedelta(days=7)
            data.astype(str).to_sql('ActFromKADS', con=connection, if_exists = 'append', index=False, schema="dbo")
        else: 
            newData = data[~data['ACTNO'].isin(dfAct['ACTNO'])]
            dfAct = dfAct.drop(columns=['LastUpdate'],axis=1)
            data = data.drop(columns=['LastUpdate'],axis=1)
            dataMask = data.merge(dfAct, how='outer', indicator=True)
            
            dataMask_diff = dataMask.loc[lambda x : x['_merge'] != 'both']
            dataMask_diff_left = dataMask_diff.loc[lambda x : x['_merge'] != 'right_only']
            #Check ActSts with ActNO
            if len(dataMask_diff_left.index)>0:
                print("Check 1")
                for index , row in dataMask_diff_left.iterrows():
                    print(row['ACTNO'],row['ActSts'])
                    updateAct = sa_text("UPDATE ActFromKADS SET [ActSts]=:actSts , [LastUpdate]=:lastupdate WHERE [ACTNO]=:actNo")
                    engineAct.execute(updateAct, actNo=row['ACTNO'], actSts=row['ActSts'], lastupdate=(datetime.now()).strftime("%Y-%m-%d %H:%M:%S"))
                mask = (data[~data['ACTNO'].isin(dfAct['ACTNO'])])
                year = mask['Period'].str.slice(start=0,stop=4)
                month = mask['Period'].str.slice(start=4,stop=6)
                mask['exp_date'] = pd.to_datetime((year+"-"+month), format='%Y-%m') + MonthEnd(1) + timedelta(days=7)
                mask['LastUpdate'] = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
                print(mask)
                mask.astype(str).to_sql('ActFromKADS', con=connection, if_exists = 'append', index=False, schema="dbo")
            elif len(dfAct.index)==0 or len(newData.index)>0:
                print("Check 2")
                data.astype(str).to_sql('ActFromKADS', con=connection, if_exists = 'append', index=False, schema="dbo")
                print(data)