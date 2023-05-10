import logging
import azure.functions as func
from . import sendmail

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    userid = req.params.get('userid')
    fullname = req.params.get('fullname')
    status = req.params.get('status')

    # if not fullname:
    #     try:
    #         req_body = req.get_json()
    #     except ValueError:
    #         pass
    #     else:
    #         fullname = req_body.get('fullname')

    if fullname:
        sendmail.gmail_send_message()
        return func.HttpResponse(f"Hello, {fullname}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
