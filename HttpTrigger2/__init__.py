import logging

import azure.functions as func
from . import linenotitest


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    DashboardName = req.params.get('DashboardName')
    ViewId = req.params.get('ViewId')
    Token = req.params.get('Token')
    FilterName = req.params.get('FilterName')
    FilterValue = req.params.get('FilterValue')
    Time = req.params.get('Time')
    Message = req.params.get('Message')
    if not DashboardName:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            DashboardName = req_body.get('DashboardName')

    if DashboardName:
        linenotitest.run()
        #return func.HttpResponse(f"Hello, {DashboardName}. {ViewId} {Token} {FilterName} {FilterValue} {Time} {Message}")
        return [DashboardName,ViewId,Token,FilterName,FilterValue,Time,Message]
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )