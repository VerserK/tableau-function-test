import pandas as pd
# from azure.storage.blob import BlobServiceClient, __version__

# CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=dwhwebstorage;AccountKey=A8aP+xOBBD5ahXo9Ch6CUvzsqkM5oyGn1/R3kcFcNSrZw4aU0nE7SQCBhHQFYif1AEPlZ4/pAoP/+AStKRerPQ==;EndpointSuffix=core.windows.net"
# CONTAINERNAME = "partsdwh-log"
# BLOBNAME = "PartsDWHLog.xlsx"
# LOCALFILENAME = "PartsDWHLog.xlsx" 

# blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
# container_client = blob_service_client.get_container_client(CONTAINERNAME)
# blob_client = blob_service_client.get_blob_client(container = CONTAINERNAME, blob=BLOBNAME)

# #READ PRODUCTS FILE
# f = open(LOCALFILENAME, "wb")
# f.write(blob_client.download_blob().content_as_bytes())
# f.close()
# df = pd.read_excel(r''+LOCALFILENAME)
# print(df)

# df = pd.read_csv(r"\\178.50.0.170\E\MyName\test.csv")
df = pd.read_excel(r'\\172.31.1.98\Z\PartsDWH_Log\PartsDWHLog.xlsx',index_col='Table')
print(df)