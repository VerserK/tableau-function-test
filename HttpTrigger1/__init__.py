import logging
import pandas as pd
import azure.functions as func
from . import mailnotiWithSQL
from datetime import datetime,timedelta


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    Enable = req.params.get('Enable')
    MailGroup = req.params.get('MailGroup')
    type = req.params.get('type')
    ID_Json = req.params.get('ID')
    ImageWidth = req.params.get('ImageWidth')
    filterName = req.params.get('filterName')
    filterValue = req.params.get('filterValue')
    imageName = req.params.get('imageName')
    CRON_Json = req.params.get('CRON')
    from_value = req.params.get('from')
    to = req.params.get('to')
    cc = req.params.get('cc')
    bcc = req.params.get('bcc')
    Subject = req.params.get('Subject')
    Content = req.params.get('Content')
    filterName_list =[]
    filterValue_list=[]
    if Enable is None:
        Enable_list = ''
    else:
        Enable_list = Enable.split(",")

    if MailGroup is None:
        MailGroup_list = ''
    else:
        MailGroup_list = MailGroup.split(",")

    if type is None:
        type_list = ''
    else:
        type_list = type.split(",")

    if ID_Json is None:
        ID_Json_list = ''
    else:
        ID_Json_list = ID_Json.split(",")

    if ImageWidth is None: 
        ImageWidth_list = ''
    else:
        ImageWidth_list = ImageWidth.split(",")

    if filterName is None:
        filterName_list=''
    else:
        for row in MailGroup_list:
            filterName_list.append(filterName)
        logging.info(filterName_list)

    if filterValue is None:
        filterValue_list = ''
    else:
        for row in MailGroup_list:
            filterValue_list.append(filterValue)
        logging.info(filterValue)

    if imageName is None:
        imageName_list = ''
    else:
        imageName_list = imageName.split(",")

    if CRON_Json is None:
        CRON_Json_list = ''
    else:
        CRON_Json_list = CRON_Json.split(",")
    
    if from_value is None:
        from_value_list = ''
    else:
        from_value_list = from_value

    if not Enable:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            Enable = req_body.get('Enable')

    #Convert to Pandas Dataframe
    df = pd.DataFrame({
    'Enable' : Enable_list,
    'MailGroup' : MailGroup_list,
    'type' : type_list,
    'ID' : ID_Json_list,
    'ImageWidth' : ImageWidth_list,
    'filterName' : filterName_list,
    'filterValue' : filterValue_list,
    'imageName' : imageName_list,
    'CRON' : CRON_Json_list,
    'from' : from_value_list,
    'to' : to,
    'cc' : cc,
    'bcc' : bcc,
    'Subject' : Subject,
    'Content' : Content
    })
    df.drop(df[df.Enable != 'x'].index, inplace=True)
    logging.info(df['filterName'])
    logging.info(df['filterValue'])
    groups = df.groupby('MailGroup')
    for name, group in groups:
        to = ''
        cc = ''
        bcc = ''
        Subject = ''
        message = ''
        m_from = ''
        iwidth = '500'
        file_list = list()
        valid = False
        for index, row in group.iterrows():
            print(index)
            valid = True
            to = row['to']
            cc = row['cc']
            bcc = row['bcc']
            iwidth = row['ImageWidth']
            Subject = row['Subject']
            today = datetime.today()
            todayStr = today.strftime("%d %B %Y")
            Subject = Subject.replace('(date)',todayStr)
            today = datetime.today() - timedelta(days=1)
            todayStr = today.strftime("%d %B %Y")
            Subject = Subject.replace('(-date)',todayStr)
            today = datetime.today()
            todayStr = today.strftime("%B %Y")
            Subject = Subject.replace('(month)',todayStr)
            today = datetime.today() - timedelta(days=30)
            todayStr = today.strftime("%B %Y")
            Subject = Subject.replace('(-month)',todayStr)
            if row['from'] =='':
                m_from = '"SKC, Dashboard"<skc_g.dashboard@kubota.com>'
            else:
                m_from = row['from']
            if row['ID'] != '':
                if row['type'] == 'file':
                    file = mailnotiWithSQL.gfileGET(row['ID'])
                if row['type'] == 'dashboard':
                    if row['imageName'] == '':
                        logging.info(row['filterName'])
                        logging.info(row['filterValue'])
                        file = mailnotiWithSQL.tableau_get_img(row['ID'],row['filterName'],row['filterValue'],'temp-'+str(index))
                    else:
                        file = mailnotiWithSQL.tableau_get_img(row['ID'],row['filterName'],row['filterValue'],row['imageName'])
                if row['type'] == 'excel':
                    if row['imageName'] == '':
                        file = mailnotiWithSQL.tableau_get_xls(row['ID'],row['filterName'],row['filterValue'],'temp-'+str(index))
                    else:
                        file = mailnotiWithSQL.tableau_get_xls(row['ID'],row['filterName'],row['filterValue'],row['imageName'])
                if row['type'] == 'folder':
                    file_list.extend(mailnotiWithSQL.gfileGETfolder(row['ID']))
                else:
                    file_list.append(file)
            txt_list = row['Content'].split('(nl)')
            message = ''
            for text in txt_list:
                message = message + text + '\n'
            today = datetime.today()
            todayStr = today.strftime("%d %B %Y")
            message = message.replace('(date)',todayStr)
            today = datetime.today() - timedelta(days=1)
            todayStr = today.strftime("%d %B %Y")
            message = message.replace('(-date)',todayStr)
            today = datetime.today()
            todayStr = today.strftime("%B %Y")
            message = message.replace('(month)',todayStr)
            today = datetime.today() - timedelta(days=30)
            todayStr = today.strftime("%B %Y")
            message = message.replace('(-month)',todayStr)
    if valid:
        msg = mailnotiWithSQL.create_message_with_attachment(m_from,to,cc,bcc,Subject,message,file_list,iwidth)
        mailnotiWithSQL.send_message('me',msg)
    if Enable:
        return func.HttpResponse("<script>alert('Sent Eamil Successfully !');</script>")
        # return func.HttpResponse[{DashboardName}]
    else:
        return func.HttpResponse("<script>alert('Sent Eamil Unsuccessful !');</script>")