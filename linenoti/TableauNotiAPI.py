# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 13:52:56 2021
@author: methee.s
"""
import requests
from datetime import datetime,timedelta
from dateutil.relativedelta import *
import ast

def GetViewId(dashboard):
    server = 'https://prod-apnortheast-a.online.tableau.com/api/3.14/'
    urlHis = server + "auth/signin"
    headers = {"Content-Type": "application/json",
               "Accept":"application/json"}
    payload = { "credentials": {
                        		"personalAccessTokenName": "MailNotify",
                        		"personalAccessTokenSecret": "spVlY5UoTiyqiN35jkpD5Q==:Ot21M39dqA5T8ZlpbaX9LclGsNMtdZ7q",
                        		"site": {
                        			"contentUrl": "skctableau"
                        		}
                }
        }
    res = requests.post(urlHis, headers=headers, json = payload)
    response =  res.json()
    token = response['credentials']['token']
    site_id = response['credentials']['site']['id']
  
    url = server + '/sites/'+site_id+'/views?filter=viewUrlName:eq:' + dashboard
    headers = {"Content-Type": "application/json",
               "Accept":"application/json",
               "X-Tableau-Auth": token}
    res = requests.get(url, headers=headers, json = {})
    response =  res.json()
    if len(response['views']) == 0 :
        return '','','',''
    elif len(response['views']['view']) > 1 :
        return response['views']['view'][0]['id'],site_id,headers,server
    else :
        return response['views']['view'][0]['id'],site_id,headers,server

def GetImage(dashboard,Id,filterName,filterValue,LineToken,message):
    if dashboard == 'MESSAGE':
        LineUrl = 'https://notify-api.line.me/api/notify'
        #LineToken = 'QDd6ExB9L9onVWb2sze4DfStpiKHB6DXTVCpV2teXEk'
        LineHeaders = {'Authorization':'Bearer '+ LineToken}
        payload = {'message':message}
        payload = str(payload)
        today = datetime.today()
        todayStr = today.strftime("%d %B %Y")
        payload = payload.replace('(date)',todayStr)
        today = datetime.today() - timedelta(days=1)
        todayStr = today.strftime("%d %B %Y")
        payload = payload.replace('(-date)',todayStr)
        today = datetime.today()
        todayStr = today.strftime("%B %Y")
        payload = payload.replace('(month)',todayStr)
        today = datetime.today() - relativedelta(months=1)
        todayStr = today.strftime("%B %Y")
        payload = payload.replace('(-month)',todayStr)
        payload = ast.literal_eval(payload)
        print(payload)
        resp = requests.post(LineUrl, headers=LineHeaders , data = payload)
        print(resp)
    else:
        view_id,site_id,headers,server = GetViewId(dashboard)
        if Id != '':
            view_id = Id
        if view_id != '':
            if filterName is None:
                url = server +  '/sites/'+site_id+'/views/'+view_id+'/image' + '?maxAge=1'+'&resolution=high'
            else:
                url = server +  '/sites/'+site_id+'/views/'+view_id+'/image' + '?vf_'+filterName+'='+filterValue+'&maxAge=1'+'&resolution=high&sort=ส่วน:asc'
            #url = server +  '/sites/'+site_id+'/views/'+view_id+'/image' + '?vf_สายงาน=สายงานวางแผนและควบคุม/CFO'
            res = requests.get(url, headers=headers, json = {})
            print(view_id)
            #Send to Line
            LineUrl = 'https://notify-api.line.me/api/notify'
            #LineToken = 'QDd6ExB9L9onVWb2sze4DfStpiKHB6DXTVCpV2teXEk'
            LineHeaders = {'Authorization':'Bearer '+ LineToken}
            payload = {'message':message}
            payload = str(payload)
            today = datetime.today()
            todayStr = today.strftime("%d %B %Y")
            payload = payload.replace('(date)',todayStr)
            today = datetime.today() - timedelta(days=1)
            todayStr = today.strftime("%d %B %Y")
            payload = payload.replace('(-date)',todayStr)
            today = datetime.today()
            todayStr = today.strftime("%B %Y")
            payload = payload.replace('(month)',todayStr)
            today = datetime.today() - relativedelta(months=1)
            todayStr = today.strftime("%B %Y")
            payload = payload.replace('(-month)',todayStr)
            payload = ast.literal_eval(payload)
            file = {'imageFile':res.content}
            print(payload)
            resp = requests.post(LineUrl, headers=LineHeaders , data = payload , files = file)
            print(resp)