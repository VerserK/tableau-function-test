import logging
import json
import requests
import azure.functions as func
from . import mailnotiWithSQL


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    server = 'https://tableauauto.azurewebsites.net/testsend'
    headers = {"Content-Type": "application/json",
               "Accept":"application/json"}
    res = requests.get(server, headers=headers)
    return func.HttpResponse(res, status_code=200)