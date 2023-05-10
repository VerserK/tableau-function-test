import logging
import azure.functions as func
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError
from azure.storage.blob import BlobServiceClient, __version__
import os,tempfile,base64
from email.message import EmailMessage


def gmail_send_message():
    #html contact
    html = """<!DOCTYPE html>
    <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Email</title>
    <style>
        .responsive {
        width: 100%;
        height: auto;
        }
    </style>
    </head>
    <body>
    <p>งานเข้าแล้วป่าน รีบไปปรับให้ลูกค้าด่วนๆๆ</p>
    <p>จึงเรียนมาเพื่อทราบและดำเนินการโดยเร่งด่วน</p>
    </body>
    </html>
    """
    creds = None
    # If modifying these scopes, delete the file token.json.
    SCOPES = ['https://www.googleapis.com/auth/gmail.compose']
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.

    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string('DefaultEndpointsProtocol=https;AccountName=d710rgsi01diag;AccountKey=nr/2Yn9nN9bWr0GNNSiNvBbN91MfYpkcIK0+9xcrYMdrFttcEAqV4kBBGGd8ehk+BRZ0gfe0iOTeoYVlRNbXOw==;EndpointSuffix=core.windows.net')

    # Create a unique name for the container
    container_name = 'methee-google-file'

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob='gmail-tableau-token.json')
    creds_path = os.path.join(tempfile.gettempdir(), 'gmail-tableau-token.json')

    with open(creds_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

    creds = Credentials.from_authorized_user_file(creds_path, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            exit
        # Save the credentials for the next run
        with open(creds_path, 'w') as token:
            token.write(creds.to_json())
        with open(creds_path, "rb") as data:
            blob_client.upload_blob(data,overwrite=True)

    try:
        service = build('gmail', 'v1', credentials=creds)
        message = EmailMessage()
        message.set_content('simple text would go here - This is a fallback for html content')
        message.add_alternative(html, subtype='html')
        message['To'] = ['natchaporn.kh@kubota.com']
        message['cc'] = ['akarawat.p@kubota.com','chonnikan.r@kubota.com']
        message['From'] = '"SKC, Dashboard"<skc_g.dashboard@kubota.com>'
        message['Subject'] = 'งานเข้าแล้วป่าน จัดการด่วนๆ'

        # encoded message
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()) \
            .decode()

        create_message = {
            'raw': encoded_message
        }
        # pylint: disable=E1101
        send_message = (service.users().messages().send
                        (userId="me", body=create_message).execute())
        print(F'Message Id: {send_message["id"]}')
    except HttpError as error:
        print(F'An error occurred: {error}')
        send_message = None
    return send_message

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    userid = req.params.get('userid')
    fullname = req.params.get('fullname')
    status = req.params.get('status')


    if fullname:
        gmail_send_message()
        return func.HttpResponse(f"Hello, {fullname}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
