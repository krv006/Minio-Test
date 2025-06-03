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
    F.[FilePath]
   ,F.[FileName]
   ,O.[Name]
FROM 
    [CottonDb].[dbo].[Files] F
JOIN 
    [CottonDb].[dbo].[Organizations] O ON F.OwnerId = O.Id
where 
	    F.[FilePath] IS NOT NULL or F.[FilePath] LIKE '%.xlsx'
"""

df = pd.read_sql(query, conn)
conn.close()

file_paths = df['FilePath'].dropna().tolist()

if not file_paths:
    print("❗ Hech qanday .xlsx fayl topilmadi.")
else:
    print(f"✅ {len(file_paths)} ta .xlsx fayl topildi.")

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


def download_file(object_key):
    try:
        file_name = os.path.basename(object_key)
        local_path = os.path.join(local_directory, file_name)
        s3.Bucket(bucket_name).download_file(object_key, local_path)
        print(f"✅ Yuklandi: {object_key}")
    except Exception as e:
        print(f"❌ Xatolik: {object_key} | {e}")


if file_paths:
    print("🚀 Yuklab olish boshlandi...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(download_file, file_paths)
else:
    print("❗ Yuklab olish uchun .xlsx fayllar yo‘q.")

"""

json ga ogirish kerak boladi -> ular qib beradi

valid invalid tekshirish kerak boladi

"""
