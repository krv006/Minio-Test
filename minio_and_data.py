import os
from concurrent.futures import ThreadPoolExecutor

import boto3
import pandas as pd
import pyodbc
from botocore.client import Config

server = '192.168.111.14'
database = 'CottonDb'
username = 'sa'
password = 'AX8wFfMQrR6b9qdhHt2eYS'
driver = '{SQL Server}'

conn_str = f"""
    DRIVER={driver};
    SERVER={server};
    DATABASE={database};
    UID={username};
    PWD={password};
"""
conn = pyodbc.connect(conn_str)

query = """
SELECT 
    F.[FilePath],
    F.[FileName],
    O.[Name] AS OrgName
FROM 
    [CottonDb].[dbo].[Files] F
JOIN 
    [CottonDb].[dbo].[Organizations] O ON F.OwnerId = O.Id
WHERE 
    F.[FilePath] IS NOT NULL OR F.[FilePath] LIKE '%.xlsx'
"""

df = pd.read_sql(query, conn)
conn.close()

if df.empty:
    print("❗ Hech qanday .xlsx fayl topilmadi.")
else:
    print(f"✅ {len(df)} ta .xlsx fayl topildi.")

s3 = boto3.resource(
    's3',
    endpoint_url='https://minio-cdn.uzex.uz',
    aws_access_key_id='cotton',
    aws_secret_access_key='xV&q+8AHHXBSK}',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

bucket_name = 'cotton'
local_directory = 'downloaded_files'

if not os.path.exists(local_directory):
    os.makedirs(local_directory)

def download_file(row):
    try:
        object_key = row["FilePath"]
        file_name = row["FileName"]
        org_name = row["OrgName"]

        safe_org = "".join(c for c in org_name if c.isalnum() or c in (" ", "_")).strip().replace(" ", "_")
        safe_file = "".join(c for c in file_name if c.isalnum() or c in (" ", "_", "-")).strip().replace(" ", "_")

        if not safe_file.lower().endswith(".xlsx"):
            safe_file += ".xlsx"

        new_file_name = f"{safe_org}_{safe_file}"
        local_path = os.path.join(local_directory, new_file_name)

        s3.Bucket(bucket_name).download_file(object_key, local_path)
        print(f"✅ Yuklandi: {object_key} -> {new_file_name}")
    except Exception as e:
        print(f"❌ Xatolik: {object_key} | {e}")

if not df.empty:
    print("🚀 Yuklab olish boshlandi...")
    rows = df.to_dict("records")
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(download_file, rows)
else:
    print("❗ Yuklab olish uchun fayl yo‘q.")
