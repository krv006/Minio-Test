import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import boto3
import pandas as pd
import pyodbc
from botocore.client import Config
from sqlalchemy import create_engine, types
from tqdm import tqdm

# 🔧 Logging sozlamalari
logging.basicConfig(
    filename='excel_to_db.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# 🔧 To‘g‘ridan-to‘g‘ri sozlamalar
source_server = '192.168.111.14'
source_database = 'CottonDb'
target_database = 'Test_Xlsx_File'
username = 'sa'
password = 'AX8wFfMQrR6b9qdhHt2eYS'
driver = '{ODBC Driver 17 for SQL Server}'

minio_endpoint = 'https://minio-cdn.uzex.uz'
minio_access_key = 'cotton'
minio_secret_key = 'xV&q+8AHHXBSK}'
bucket_name = 'cotton'
local_directory = 'downloaded_files'

# 🛠 Bazaga ulanish
source_conn_str = f"""
    DRIVER={driver};
    SERVER={source_server};
    DATABASE={source_database};
    UID={username};
    PWD={password};
"""
try:
    conn = pyodbc.connect(source_conn_str)
    logging.info("Ma'lumotlar bazasiga ulanish muvaffaqiyatli.")
except Exception as e:
    logging.error(f"DB ulanish xatosi: {e}")
    print(f"❌ DB ulanish xatosi: {e}")
    exit()

query = """
SELECT 
    F.[Id] AS FileId, F.[ParentId], F.[CreatedAt], F.[FilePath],
    F.[FileName], O.[Name] AS OrgName
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

# 🔗 MinIO ulanish
s3 = boto3.resource(
    's3',
    endpoint_url=minio_endpoint,
    aws_access_key_id=minio_access_key,
    aws_secret_access_key=minio_secret_key,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)
os.makedirs(local_directory, exist_ok=True)


# 📥 Fayl yuklash funksiyasi
def download_file(row):
    object_key = row["FilePath"]
    file_name = row["FileName"]
    org_name = "".join(c for c in row["OrgName"] if c.isalnum() or c in (" ", "_")).replace(" ", "_")
    safe_file = "".join(c for c in file_name if c.isalnum() or c in (" ", "_", "-")).replace(" ", "_").lower().replace(
        ".xlsx", "")
    new_file_name = f"{org_name}_{safe_file}_{row['FileId']}.xlsx"
    local_path = os.path.join(local_directory, new_file_name)

    try:
        s3.Bucket(bucket_name).download_file(object_key, local_path)
        return {
            'file_name': new_file_name,
            'file_id': row["FileId"],
            'parent_id': row["ParentId"],
            'created_at': row["CreatedAt"],
            'is_updated': pd.notna(row["ParentId"])
        }
    except Exception as e:
        logging.error(f"❌ Yuklash xatosi: {object_key} | {e}")
        return None


# 📥 Parallel yuklash
print("🚀 Fayllarni yuklab olish boshlandi...")
rows = df.to_dict("records")
downloaded_files = []

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(download_file, row) for row in rows]
    for future in tqdm(as_completed(futures), total=len(futures), desc="Yuklab olinmoqda"):
        result = future.result()
        if result:
            downloaded_files.append(result)

if not downloaded_files:
    print("❌ Fayllar yuklanmadi.")
    exit()

# 🗑 Eski jadvallarni o‘chirish
target_conn_str = f"""
    DRIVER={driver};
    SERVER={source_server};
    DATABASE={target_database};
    UID={username};
    PWD={password};
"""
conn = pyodbc.connect(target_conn_str)
cursor = conn.cursor()
cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo'")
tables = [row[0] for row in cursor.fetchall()]
for table in tables:
    cursor.execute(f"DROP TABLE [dbo].[{table}]")
    print(f"🗑 O‘chirildi: {table}")
conn.commit()
cursor.close()
conn.close()

# 🧠 Bazaga yozish
engine = create_engine(
    f"mssql+pyodbc://{username}:{password}@{source_server}/{target_database}?driver=ODBC+Driver+17+for+SQL+Server"
)
current_date = datetime.now()

print("🗂 Bazaga yozilmoqda...")
for file_info in tqdm(downloaded_files, desc="Yozilmoqda"):
    file_name = file_info['file_name']
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

        # Qo‘shimcha ustunlar
        df_xlsx['FileId'] = file_info['file_id']
        df_xlsx['ParentId'] = file_info['parent_id']
        df_xlsx['CreatedAt'] = file_info['created_at']
        df_xlsx['ResevedDate'] = current_date
        df_xlsx['IsDel'] = file_info['is_updated']

        # Dtype mapping
        dtype_mapping = {}
        for col in df_xlsx.columns:
            if col in ['FileId', 'ParentId']:
                dtype_mapping[col] = types.BIGINT()
            elif col in ['CreatedAt', 'ResevedDate']:
                dtype_mapping[col] = types.DateTime()
            elif col == 'IsDel':
                dtype_mapping[col] = types.Boolean()
            elif pd.api.types.is_integer_dtype(df_xlsx[col]):
                dtype_mapping[col] = types.BIGINT()
            elif pd.api.types.is_float_dtype(df_xlsx[col]):
                dtype_mapping[col] = types.Float(precision=18)
            elif pd.api.types.is_datetime64_any_dtype(df_xlsx[col]):
                dtype_mapping[col] = types.DateTime()
            else:
                dtype_mapping[col] = types.NVARCHAR(length=255)

        # Bazaga yozish
        df_xlsx.to_sql(
            table_name,
            con=engine,
            if_exists='replace',
            index=False,
            dtype=dtype_mapping,
            method='multi',
            chunksize=1000
        )
        print(f"✅ Yozildi: {table_name}")

    except Exception as e:
        logging.error(f"Yozishda xatolik: {file_name} | {e}")
        print(f"❌ Yozishda xatolik: {file_name} | {e}")
