import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import pandas as pd
import pyodbc
from botocore.client import Config
from sqlalchemy import create_engine
from tqdm import tqdm

logging.basicConfig(
    filename='excel_to_db.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

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

s3 = boto3.resource(
    's3',
    endpoint_url=minio_endpoint,
    aws_access_key_id=minio_access_key,
    aws_secret_access_key=minio_secret_key,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)
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

target_conn_str = f"""
        DRIVER={driver};
        SERVER={source_server};
        DATABASE={target_database};
        UID={username};
        PWD={password};
    """

conn = pyodbc.connect(target_conn_str)
cursor = conn.cursor()


def check_and_update_existing_file(file_info, df_xlsx):
    cursor.execute(f"SELECT COUNT(1) FROM [dbo].[{file_info['file_name']}] WHERE FileId = ?", file_info["file_id"])
    exists = cursor.fetchone()[0]

    if exists:
        print(f"✅ {file_info['file_name']} mavjud. Yangilanmoqda...")
        cursor.execute(
            f"UPDATE [dbo].[{file_info['file_name']}] SET {', '.join([f'{col} = ?' for col in df_xlsx.columns])} WHERE FileId = ?",
            *df_xlsx.values.flatten(), file_info["file_id"])
    else:
        print(f"✅ {file_info['file_name']} yangi fayl qo‘shilyapti...")
        df_xlsx.to_sql(
            file_info['file_name'],
            con=engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )

    conn.commit()


engine = create_engine(
    f"mssql+pyodbc://{username}:{password}@{source_server}/{target_database}?driver=ODBC+Driver+17+for+SQL+Server"
)

for file_info in tqdm(downloaded_files, desc="Yozilmoqda"):
    file_name = file_info['file_name']
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
        check_and_update_existing_file(file_info, df_xlsx)

    except Exception as e:
        logging.error(f"Yozishda xatolik: {file_name} | {e}")
        print(f"❌ Yozishda xatolik: {file_name} | {e}")


"""
OwnerId ni xam tiqib ketish kere boladi
"""