import logging
import azure.functions as func
import json
from . import sendmail

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    data = req.get_json()

    if not data:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            data = req_body.get('data')

    if data:
        # sendmail.gmail_send_message(userid,fullname,status,oldnumber,newnumber,email)
        return func.HttpResponse(f"Hello, {json.dumps(data)}. successfully.", status_code = 200)
    else:
        return func.HttpResponse(
             "Fail not pass parameter fullname in url",
             status_code=400
        )
