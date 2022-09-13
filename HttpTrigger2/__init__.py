import logging

import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    DashboardName = req.params.get('DashboardName')
    if not DashboardName:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            DashboardName = req_body.get('DashboardName')

    if DashboardName:
        return func.HttpResponse(f"Hello, {DashboardName}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
