import configparser
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

# Logging sozlamalari
logging.basicConfig(
    filename='excel_to_db.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# Konfiguratsiya faylini o'qish
config = configparser.ConfigParser()
config.read('config.ini')

# Ma'lumotlar bazasi sozlamalari
source_server = config.get('Database', 'source_server', fallback='192.168.111.14')
source_database = config.get('Database', 'source_database', fallback='CottonDb')
target_database = config.get('Database', 'target_database', fallback='Test_Xlsx_File')
username = config.get('Database', 'username', fallback='sa')
password = config.get('Database', 'password', fallback='AX8wFfMQrR6b9qdhHt2eYS')
driver = config.get('Database', 'driver', fallback='{ODBC Driver 17 for SQL Server}')

# MinIO sozlamalari
minio_endpoint = config.get('MinIO', 'endpoint_url', fallback='https://minio-cdn.uzex.uz')
minio_access_key = config.get('MinIO', 'aws_access_key_id', fallback='cotton')
minio_secret_key = config.get('MinIO', 'aws_secret_access_key', fallback='xV&q+8AHHXBSK}')
bucket_name = config.get('MinIO', 'bucket_name', fallback='cotton')
local_directory = config.get('General', 'local_directory', fallback='downloaded_files')

# 1. Ma'lumotlar bazasiga ulanish
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
    logging.error(f"Ma'lumotlar bazasiga ulanishda xatolik: {e}")
    print(f"❌ Ma'lumotlar bazasiga ulanishda xatolik: {e}")
    exit()

query = """
SELECT 
    F.[Id] AS FileId,
    F.[ParentId],
    F.[CreatedAt],
    F.[FilePath],
    F.[FileName],
    O.[Name] AS OrgName
FROM 
    [CottonDb].[dbo].[Files] F
JOIN 
    [CottonDb].[dbo].[Organizations] O ON F.OwnerId = O.Id
WHERE 
    F.[FilePath] IS NOT NULL AND F.[FilePath] LIKE '%.xlsx'
"""

try:
    df = pd.read_sql(query, conn)
    logging.info(f"{len(df)} ta .xlsx fayl topildi.")
except Exception as e:
    logging.error(f"SQL so'rovida xatolik: {e}")
    print(f"❌ SQL so'rovida xatolik: {e}")
    conn.close()
    exit()

conn.close()

if df.empty:
    logging.warning("Hech qanday .xlsx fayl topilmadi.")
    print("❗ Hech qanday .xlsx fayl topilmadi.")
    exit()
else:
    print(f"✅ {len(df)} ta .xlsx fayl topildi.")

# 2. MinIO sozlamalari
try:
    s3 = boto3.resource(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    logging.info("MinIO ga ulanish muvaffaqiyatli.")
except Exception as e:
    logging.error(f"MinIO ga ulanishda xatolik: {e}")
    print(f"❌ MinIO ga ulanishda xatolik: {e}")
    exit()

os.makedirs(local_directory, exist_ok=True)


# 3. Fayl yuklash funksiyasi
def download_file(row, conn):
    object_key = row["FilePath"]
    file_name = row["FileName"]
    org_name = row["OrgName"]
    file_id = row["FileId"]
    parent_id = row["ParentId"]
    created_at = row["CreatedAt"]

    try:
        # Tashkilot nomini tozalash
        safe_org = "".join(c for c in org_name if c.isalnum() or c in (" ", "_")).strip().replace(" ", "_")

        # Fayl nomini tozalash
        safe_file = "".join(c for c in file_name if c.isalnum() or c in (" ", "_", "-")).strip().replace(" ", "_")

        # Kengaytmani to'g'rilash
        safe_file = safe_file.lower().replace(".xlsx", "").replace("xlsx", "").strip("_")

        # Standart .xlsx kengaytmasini qo'shish
        new_file_name = f"{safe_org}_{safe_file}.xlsx"
        local_path = os.path.join(local_directory, new_file_name)

        # ParentId bo'lsa, mos faylni topish
        parent_file_path = None
        is_updated = False
        if pd.notna(parent_id):
            cursor = conn.cursor()
            parent_query = """
            SELECT [FilePath]
            FROM [CottonDb].[dbo].[Files]
            WHERE [Id] = ?
            """
            cursor.execute(parent_query, parent_id)
            result = cursor.fetchone()
            if result:
                parent_file_path = result[0]
                is_updated = True
            cursor.close()

        # Faylni yuklab olish
        if parent_file_path:
            # Parent faylni yuklash (yangilash uchun)
            s3.Bucket(bucket_name).download_file(parent_file_path, local_path)
            logging.info(f"Parent fayl yuklandi: {parent_file_path} -> {new_file_name}")
            print(f"✅ Parent fayl yuklandi: {parent_file_path} -> {new_file_name}")
        else:
            # ParentId yo'q bo'lsa, o'z faylini yuklash
            s3.Bucket(bucket_name).download_file(object_key, local_path)
            logging.info(f"Yuklandi: {object_key} -> {new_file_name}")
            print(f"✅ Yuklandi: {object_key} -> {new_file_name}")

        return {
            'file_name': new_file_name,
            'file_id': file_id,
            'parent_id': parent_id,
            'created_at': created_at,
            'is_updated': is_updated  # Yangilanganligini belgilash
        }
    except Exception as e:
        logging.error(f"Yuklashda xatolik: {object_key} | {e}")
        print(f"❌ Xatolik: {object_key} | {e}")
        return None


# 4. Fayllarni parallel yuklab olish
print("🚀 Yuklab olish boshlandi...")
rows = df.to_dict("records")
downloaded_files = []

# MinIO uchun ulanishni ochish
conn = pyodbc.connect(source_conn_str)
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(download_file, row, conn) for row in rows]
    for future in tqdm(as_completed(futures), total=len(futures), desc="Fayllarni yuklash"):
        result = future.result()
        if result:
            downloaded_files.append(result)
conn.close()

if not downloaded_files:
    logging.warning("Hech qanday fayl yuklanmadi.")
    print("❌ Hech qanday fayl yuklanmadi.")
    exit()

# 5. Ma'lumotlar bazasidagi barcha jadvallarni o'chirish
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

    # Barcha jadvallarni olish
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo'")
    tables = [row[0] for row in cursor.fetchall()]

    if not tables:
        logging.info("Ma'lumotlar bazasida jadvallar topilmadi.")
        print("ℹ️ Ma'lumotlar bazasida jadvallar topilmadi.")

    # Har bir jadvalni o'chirish
    for table in tables:
        cursor.execute(f"DROP TABLE [dbo].[{table}]")
        logging.info(f"Jadval o'chirildi: {table}")
        print(f"🗑 Jadval o'chirildi: {table}")

    conn.commit()
    cursor.close()
    conn.close()
except Exception as e:
    logging.error(f"Jadvallarni o'chirishda xatolik: {e}")
    print(f"❌ Jadvallarni o'chirishda xatolik: {e}")
    exit()

# 6. Ma'lumotlar bazasiga yozish (bo‘sh fayllarni o‘tkazib yuborish)
engine = create_engine(
    f"mssql+pyodbc://{username}:{password}@{source_server}/{target_database}?driver=ODBC+Driver+17+for+SQL+Server"
)

print("🗂 Bazaga yozish boshlandi...")
current_date = datetime.now()  # Hozirgi sanani olish
for file_info in tqdm(downloaded_files, desc="Fayllarni bazaga yozish"):
    file_name = file_info['file_name']
    file_id = file_info['file_id']
    parent_id = file_info['parent_id']
    created_at = file_info['created_at']
    is_updated = file_info['is_updated']
    table_name = os.path.splitext(file_name)[0].replace(" ", "_").replace("-", "_")
    local_path = os.path.join(local_directory, file_name)

    try:
        xl = pd.ExcelFile(local_path, engine='openpyxl')
        if not xl.sheet_names:
            logging.warning(f"Sheet topilmadi: {file_name}")
            print(f"⚠️ Sheet topilmadi: {file_name}")
            continue

        df_xlsx = xl.parse(xl.sheet_names[0])
        if df_xlsx.empty:
            logging.warning(f"Bo‘sh fayl: {file_name}")
            print(f"⚠️ Bo‘sh fayl: {file_name}")
            continue  # Bo‘sh faylni bazaga yozmaymiz

        df_xlsx.columns = [str(col).strip().replace(' ', '_') for col in df_xlsx.columns]

        # Qo'shimcha ustunlarni qo'shish
        df_xlsx['FileId'] = file_id
        df_xlsx['ParentId'] = parent_id if pd.notna(parent_id) else None
        df_xlsx['CreatedAt'] = created_at
        df_xlsx['ResevedDate'] = current_date
        df_xlsx['IsDel'] = is_updated  # True agar yangilangan bo'lsa, aks holda False

        dtype_mapping = {}
        for col in df_xlsx.columns:
            if col in ['FileId', 'ParentId']:
                dtype_mapping[col] = types.BIGINT()
            elif col in ['CreatedAt', 'ResevedDate']:
                dtype_mapping[col] = types.DateTime()
            elif col == 'IsDel':
                dtype_mapping[col] = types.BOOLEAN()
            elif pd.api.types.is_integer_dtype(df_xlsx[col]):
                dtype_mapping[col] = types.BIGINT()
            elif pd.api.types.is_float_dtype(df_xlsx[col]):
                dtype_mapping[col] = types.Float(precision=18)
            elif pd.api.types.is_datetime64_any_dtype(df_xlsx[col]):
                dtype_mapping[col] = types.DateTime()
            else:
                dtype_mapping[col] = types.NVARCHAR(length=255)

        df_xlsx.to_sql(table_name, con=engine, if_exists='replace', index=False, dtype=dtype_mapping)
        logging.info(f"Bazaga yozildi: {table_name} | FileId: {file_id} | IsDel: {is_updated}")
        print(f"✅ Bazaga yozildi: {table_name}")

        # Faylni o'chirish
        os.remove(local_path)
        logging.info(f"Fayl o'chirildi: {local_path}")
        print(f"🗑 Fayl o'chirildi: {local_path}")

    except Exception as e:
        logging.error(f"Bazaga yozishda xatolik: {file_name} | {e}")
        print(f"❌ Bazaga yozishda xatolik: {file_name} | {e}")

print("🎉 Tayyor: barcha fayllar bazaga yozildi.")


"""
FileId
ParentID
CreatedAt  
ResevedDate 

IsDel - > Parent ID si bor file keldi parent id boyicha bazaga yozganlarim (File id)
agar file update bolsa IsDel -> TRUE bolishi kere

"""
