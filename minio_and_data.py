import os
import pyodbc
import pandas as pd
from minio import Minio
from minio.error import S3Error
from concurrent.futures import ThreadPoolExecutor

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
FROM 
    [CottonDb].[dbo].[Files] F
JOIN 
    [CottonDb].[dbo].[Organizations] O ON F.OwnerId = O.Id

"""

df = pd.read_sql(query, conn)
conn.close()

file_paths = df['FilePath'].tolist()

if not file_paths:
    print("❗ Hech qanday .xlsx fayl topilmadi.")
else:
    print(f"✅ {len(file_paths)} ta .xlsx fayl topildi.")

client = Minio(
    endpoint="minio-cdn.uzex.uz",
    access_key="cotton",
    secret_key="xV&q+8AHHXBSK}",
    secure=True
)

bucket_name = "cotton"
local_directory = "downloaded_files"

if not os.path.exists(local_directory):
    os.makedirs(local_directory)

def download_file(object_name):
    try:
        local_file_path = os.path.join(local_directory, os.path.basename(object_name))
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        client.fget_object(bucket_name, object_name, local_file_path)
        print(f"✅ Yuklandi: {object_name}")
    except S3Error as e:
        print(f"❌ Yuklashda xatolik: {object_name} | {e}")

xlsx_files = [path for path in file_paths if path.endswith('.xlsx')]

if xlsx_files:
    print("🚀 Yuklab olish boshlandi...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(download_file, xlsx_files)
else:
    print("❗ Yuklab olish uchun .xlsx fayllar yo‘q.")
