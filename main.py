from minio import Minio
from minio.error import S3Error
from concurrent.futures import ThreadPoolExecutor
import os


def download_file(client, bucket_name, object_name, local_directory):
    try:
        local_file_path = os.path.join(local_directory, object_name)
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        client.fget_object(bucket_name, object_name, local_file_path)
        print(f"{object_name} yuklandi -> {local_file_path}")
    except S3Error as e:
        print(f"Xato {object_name} yuklashda: {e}")


client = Minio(
    endpoint="minio-cdn.uzex.uz",
    access_key="cotton",
    secret_key="xV&q+8AHHXBSK}",
    secure=True
)


bucket_name = "cotton"
prefix = "Templates/f794b5cf-d399-4d32-a0eb-fa4e32683551/2025/05/08/50baf5aecac149c180e797ee83aee850"
local_directory = "downloaded_files"

if not os.path.exists(local_directory):
    os.makedirs(local_directory)

try:
    # Prefix bo'yicha fayllarni olish
    objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
    object_names = [obj.object_name for obj in objects]

    # Parallel yuklash
    with ThreadPoolExecutor(max_workers=4) as executor:  # 4 ta parallel jarayon
        executor.map(lambda obj_name: download_file(client, bucket_name, obj_name, local_directory), object_names)

except S3Error as err:
    print(f"Xato yuz berdi: {err}")