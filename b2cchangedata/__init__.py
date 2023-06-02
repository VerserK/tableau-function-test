import logging
import azure.functions as func
import json
from urllib.parse import parse_qs
from . import sendmail

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    userid = req.params.get('userid')
    fullname = req.params.get('fullname')
    status = req.params.get('status')
    oldnumber = req.params.get('oldnumber')
    newnumber = req.params.get('newnumber')
    email = req.params.get('email')
    
    # data = req.get_json()

    # if not data:
    #     try:
    #         req_body = req.get_json()
    #     except ValueError:
    #         pass
    #     else:
    #         data = req_body.get('data')

    # userid = data['userid']
    # fullname = data['fullname']
    # status = data['status']
    # oldnumber = data['oldnumber']
    # newnumber = data['newnumber']
    # email = data['email']
    data = json.dumps(parse_qs("userid="+userid+"&fullname="+fullname+"&status="+status+"&oldnumber="+oldnumber+"&newnumber="+newnumber+"&email="+email))
    if data:
        sendmail.gmail_send_message(userid,fullname,status,oldnumber,newnumber,email)
        return func.HttpResponse(data, status_code = 200)
    else:
        return func.HttpResponse(
             "Fail not pass parameter fullname in url",
             status_code=400
        )
