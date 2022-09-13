import logging
import sys
import os
import azure.functions as func
from . import mailnotiWithSQL


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    try:
        mailnotiWithSQL.run()
        return func.HttpResponse(
                "This HTTP triggered function executed successfully.",
                status_code=200
            )
    except:
        logging.info("  error!", sys.exc_info()[0], "occurred.")
    os._exit(0)