import logging
import azure.functions as func
from . import sendmail

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    userid = req.params.get('userid')
    fullname = req.params.get('fullname')
    status = req.params.get('status')
    oldnumber = req.params.get('oldnumber')
    newnumber = req.params.get('newnumber')
    email = req.params.get('email')

    if not fullname:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            fullname = req_body.get('fullname')

    if fullname:
        sendmail.gmail_send_message(userid,fullname,status,oldnumber,newnumber,email)
        return func.HttpResponse(f"Hello, {fullname}. successfully.", status_code = 200)
    else:
        return func.HttpResponse(
             "Fail not pass parameter fullname in url",
             status_code=400
        )
