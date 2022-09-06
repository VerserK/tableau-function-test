import logging

import azure.functions as func
from . import mailnotiWithSQL


def main(warmupContext: func.Context) -> None:
    logging.info('Python HTTP trigger function processed a request.')
    mailnotiWithSQL.run()
    return func.HttpResponse("", status_code=200)