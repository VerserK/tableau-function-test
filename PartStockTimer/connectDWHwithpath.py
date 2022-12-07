import pandas as pd
from ftplib import FTP
df = pd.read_excel(r'\\172.31.8.25\SharingPathDW\PartsDWH_Log\PartsDWHLog.xlsx',index_col='Table')
print(df)