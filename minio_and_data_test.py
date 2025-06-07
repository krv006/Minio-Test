import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
import pandas as pd
import pyodbc
from botocore.client import Config
from sqlalchemy import create_engine, types

source_server = '192.168.111.14'
source_database = 'CottonDb'
username = 'sa'
password = 'AX8wFfMQrR6b9qdhHt2eYS'
driver = '{ODBC Driver 17 for SQL Server}'

source_conn_str = f"""
    DRIVER={driver};
    SERVER={source_server};
    DATABASE={source_database};
    UID={username};
    PWD={password};
"""
conn = pyodbc.connect(source_conn_str)

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
    exit()
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
os.makedirs(local_directory, exist_ok=True)


def download_file(row):
    object_key = row["FilePath"]
    file_name = row["FileName"]
    org_name = row["OrgName"]

    try:
        safe_org = "".join(c for c in org_name if c.isalnum() or c in (" ", "_")).strip().replace(" ", "_")
        safe_file = "".join(c for c in file_name if c.isalnum() or c in (" ", "_", "-")).strip().replace(" ", "_")
        safe_file = safe_file.lower().replace(".xlsx", "").replace("xlsx", "").strip("_")
        new_file_name = f"{safe_org}_{safe_file}.xlsx"
        local_path = os.path.join(local_directory, new_file_name)
        s3.Bucket(bucket_name).download_file(object_key, local_path)
        print(f"✅ Yuklandi: {object_key} -> {new_file_name}")
        return new_file_name
    except Exception as e:
        print(f"❌ Xatolik: {object_key} | {e}")
        return None


print("🚀 Yuklab olish boshlandi...")
rows = df.to_dict("records")
downloaded_files = []

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(download_file, row) for row in rows]
    for future in as_completed(futures):
        result = future.result()
        if result:
            downloaded_files.append(result)

if not downloaded_files:
    print("❌ Hech qanday fayl yuklanmadi.")
    exit()

target_database = 'Test_Xlsx_File'
target_conn_str = f"""
    DRIVER={driver};
    SERVER={source_server};
    DATABASE={target_database};
    UID={username};
    PWD={password};
"""
print("🗑 Barcha jadvallarni o'chirish boshlandi...")
try:
    conn = pyodbc.connect(target_conn_str)
    cursor = conn.cursor()

    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo'")
    tables = [row[0] for row in cursor.fetchall()]

    for table in tables:
        cursor.execute(f"DROP TABLE [dbo].[{table}]")
        print(f"🗑 Jadval o'chirildi: {table}")

    conn.commit()
    cursor.close()
    conn.close()
except Exception as e:
    print(f"❌ Jadvallarni o'chirishda xatolik: {e}")
    exit()

engine = create_engine(
    f"mssql+pyodbc://{username}:{password}@{source_server}/{target_database}?driver=ODBC+Driver+17+for+SQL+Server"
)

print("🗂 Bazaga yozish boshlandi...")
for file_name in downloaded_files:
    table_name = os.path.splitext(file_name)[0].replace(" ", "_").replace("-", "_")
    local_path = os.path.join(local_directory, file_name)

    try:
        xl = pd.ExcelFile(local_path, engine='openpyxl')
        if not xl.sheet_names:
            print(f"⚠️ Sheet topilmadi: {file_name}")
            continue

        df_xlsx = xl.parse(xl.sheet_names[0])
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
        print(f"✅ Bazaga yozildi: {table_name}")

    except Exception as e:
        print(f"❌ Bazaga yozishda xatolik: {file_name} | {e}")

print("🎉 Tayyor: barcha fayllar bazaga yozildi.")
