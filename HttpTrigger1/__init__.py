import logging

import azure.functions as func
from . import mailnotiWithSQL


def main(req: func.HttpRequest) -> func.HttpResponse:
    mailnotiWithSQL.run()