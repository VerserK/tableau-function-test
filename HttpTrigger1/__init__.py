import logging
import sys
import os
import azure.functions as func
from . import mailnotiWithSQL


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    try:
        mailnotiWithSQL.run()
        sys.path_importer_cache.clear()
        return func.HttpResponse(
                "This HTTP triggered function executed successfully.",
                status_code=200
            )
    except:
        return func.HttpResponse(
                "Error",
                status_code=400
            )