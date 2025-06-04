import os
from concurrent.futures import ThreadPoolExecutor

import boto3
import pandas as pd
import pyodbc
from botocore.client import Config
from sqlalchemy import create_engine, types


server = '192.168.111.14'
database = 'CottonDb'
username = 'sa'
password = 'AX8wFfMQrR6b9qdhHt2eYS'
driver = '{ODBC Driver 17 for SQL Server}'

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
    O.[Name]
FROM 
    [CottonDb].[dbo].[Files] F
JOIN 
    [CottonDb].[dbo].[Organizations] O ON F.OwnerId = O.Id
WHERE 
    F.[FilePath] IS NOT NULL OR F.[FilePath] LIKE '%.xlsx'
"""

df = pd.read_sql(query, conn)
conn.close()

df['CustomFileName'] = (
        df['Name'].str.replace(r'[\\/*?:"<>|]', '_', regex=True) + '_' +
        df['FileName'].str.replace(r'[\\/*?:"<>|]', '_', regex=True)
)

file_info = df[['FilePath', 'CustomFileName']].dropna().values.tolist()

if not file_info:
    print("❗ Hech qanday .xlsx fayl topilmadi.")
else:
    print(f"✅ {len(file_info)} ta .xlsx fayl topildi.")

s3 = boto3.resource(
    's3',
    endpoint_url='https://minio-cdn.uzex.uz',
    aws_access_key_id='cotton',
    aws_secret_access_key='xV&q+8AHHXBSK}',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

bucket_name = 'cotton'
local_directory = 'Test_File'

os.makedirs(local_directory, exist_ok=True)



def download_and_save_file(item):
    object_key, file_name = item
    try:
        local_path = os.path.join(local_directory, file_name)
        s3.Bucket(bucket_name).download_file(object_key, local_path)
        print(f"✅ Yuklandi: {object_key} -> {file_name}")
    except Exception as e:
        print(f"❌ Yuklashda xatolik: {object_key} | {e}")


print("🚀 Fayllarni yuklab olish boshlandi...")
with ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(download_and_save_file, file_info)


engine = create_engine(
    f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
)


print("🗂 Ma'lumotlarni bazaga joylash boshlandi...")

for _, file_name in file_info:
    table_name = os.path.splitext(file_name)[0].replace(" ", "_").replace("-", "_")
    local_path = os.path.join(local_directory, file_name)

    try:
        df_xlsx = pd.read_excel(local_path, engine='openpyxl')

        if df_xlsx.empty:
            print(f"⚠️ Bo‘sh fayl: {file_name}")
            continue

        df_xlsx.columns = [str(col).strip().replace(' ', '_') for col in df_xlsx.columns]

        dtype_mapping = {}
        for col in df_xlsx.columns:
            if pd.api.types.is_integer_dtype(df_xlsx[col]):
                dtype_mapping[col] = types.BIGINT()
            elif pd.api.types.is_float_dtype(df_xlsx[col]):
                dtype_mapping[col] = types.Float(precision=18)
            elif pd.api.types.is_datetime64_any_dtype(df_xlsx[col]):
                dtype_mapping[col] = types.DateTime()
            else:
                dtype_mapping[col] = types.NVARCHAR(length=255)

        df_xlsx.to_sql(table_name, con=engine, if_exists='replace', index=False, dtype=dtype_mapping)
        print(f"Bazaga joylandi: {table_name}")

    except Exception as e:
        print(f"Error: {file_name} | {e}")

print("Done")


"""

agar parent id si bolsa 
10 tasini parent id si bor boladi 
sort date 
va the last yuklab yubor 
parent id boyicha file topish va shu boyicha row di topaman 
xamma dani ochiraman is_delete = True
xuddi shu parent_id si bn file update qilib yuboraman


2.
yana bita column qoshib file_id bn parent_id qoyaman
xar doim null boladi ilida bolmidi 

file_id = file_id

"""