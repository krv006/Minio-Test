from minio import Minio
from minio.error import S3Error

client = Minio(
    endpoint="minio-cdn.uzex.uz",
    access_key="cotton",
    secret_key="xV&q+8AHHXBSK}",
    secure=True
)

bucket_name = "cotton"
prefix = "Templates/f794b5cf-d399-4d32-a0eb-fa4e32683551/2025/05/08/50baf5aecac149c180e797ee83aee850"

try:
    objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
    found = False
    for obj in objects:
        print(f"Fayl topildi: {obj.object_name}")
        found = True
    if not found:
        print("Berilgan yo'lda fayllar topilmadi.")
except S3Error as err:
    print(f"Xato: {err}")