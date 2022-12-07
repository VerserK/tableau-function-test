import pandas as pd
# from azure.storage.blob import BlobServiceClient, __version__

def run():
    CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=dwhwebstorage;AccountKey=A8aP+xOBBD5ahXo9Ch6CUvzsqkM5oyGn1/R3kcFcNSrZw4aU0nE7SQCBhHQFYif1AEPlZ4/pAoP/+AStKRerPQ==;EndpointSuffix=core.windows.net"
    CONTAINERNAME = "dwhdart"
    BLOBNAME = "PartField_to_Map.csv"
    LOCALFILENAME = "PartField_to_Map.csv" 

    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINERNAME)
    blob_client = blob_service_client.get_blob_client(container = CONTAINERNAME, blob=BLOBNAME)

    #READ PRODUCTS FILE
    f = open(LOCALFILENAME, "wb")
    f.write(blob_client.download_blob().content_as_bytes())
    f.close()
    df_cols = pd.read_csv(r''+LOCALFILENAME)
    print(df_cols)