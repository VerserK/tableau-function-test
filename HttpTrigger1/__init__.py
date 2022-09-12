import logging
import sys

import azure.functions as func
from . import mailnotiWithSQL


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    mailnotiWithSQL.run()
    return func.HttpResponse(sys.exit(),status_code = 200)