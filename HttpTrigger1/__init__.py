import logging
import json
import requests
import azure.functions as func
from . import mailnotiWithSQL


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    import requests

    response = requests.post('https://tableauauto.azurewebsites.net/testsend')
    print(response.text)
    return func.HttpResponse()