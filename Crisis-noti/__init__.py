import logging

import azure.functions as func
from . import EmergencyNotify_NS

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    eid = req.params.get('eid')

    if eid:
        EmergencyNotify_NS.main(eid)
        return func.HttpResponse(f"Hello, {eid}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
