import logging

import azure.functions as func
from . import mailnotiWithSQL


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    enable = req.params.get('Enable')
    if not enable:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            enable = req_body.get('enable')

    if enable:
        return func.HttpResponse(f"Hello {enable}!")
    else:
        return func.HttpResponse(
             "Please pass a name on the query string or in the request body",
             status_code=400
        )