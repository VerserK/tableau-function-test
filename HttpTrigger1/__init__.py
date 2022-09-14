from distutils import bcppcompiler, ccompiler
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
    
    if not Enable:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            Enable = req_body.get('Enable')

    #Convert to Pandas Dataframe
    df = pd.DataFrame({
    'Enable' : [Enable],
    'MailGroup' : [MailGroup],
    'type' : [type],
    'ID' : [ID_Json],
    'ImageWidth' : [ImageWidth],
    'filterName' : [filterName],
    'filterValue' : [filterValue],
    'imageName' : [imageName],
    'CRON' : [CRON_Json],
    'from' : [from_value],
    'to' : [to],
    'cc' : [cc],
    'bcc' : [bcc],
    'Subject' : [Subject],
    'Content' : [Content]
    })
    df.drop(df[df.Enable != 'x'].index, inplace=True)
    print(df)
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
            if row['from'] is None:
                m_from = '"SKC, Dashboard"<skc_g.dashboard@kubota.com>'
            else:
                m_from = row['from']
            if row['ID'] != '':
                if row['type'] == 'file':
                    file = mailnotiWithSQL.gfileGET(row['ID'])
                if row['type'] == 'dashboard':
                    if row['imageName'] == '':
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
            txt_date = message.split('(date)')
            massageDate = ''
            for textdate in txt_date:
                if txt_date.index(textdate) == len(txt_date)-1:
                    massageDate = massageDate + textdate
                    continue
                massageDate = massageDate + textdate + todayStr
            today = datetime.today() - timedelta(days=1)
            todayStr = today.strftime("%d %B %Y")
            txt_date1 = massageDate.split('(-date)')
            massageDate1 = ''
            for textdate1 in txt_date1:
                if txt_date1.index(textdate1) == len(txt_date1)-1:
                    massageDate1 = massageDate1 + textdate1
                    continue
                massageDate1 = massageDate1 + textdate1 + todayStr
            today = datetime.today()
            todayStr = today.strftime("%B %Y")
            txt_month = massageDate1.split('(month)')
            massageMonth = ''
            for textmonth in txt_month:
                if txt_month.index(textmonth) == len(txt_month)-1:
                    massageMonth = massageMonth + textmonth
                    continue
                massageMonth = massageMonth + textmonth + todayStr
    if valid:
        msg = mailnotiWithSQL.create_message_with_attachment(m_from,to,cc,bcc,Subject,massageMonth,file_list,iwidth)
        mailnotiWithSQL.send_message('me',msg)
    if Enable:
        return func.HttpResponse(f"Hello, {Enable}.")
        # return func.HttpResponse[{DashboardName}]
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )