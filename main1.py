from minio import Minio
from minio.error import S3Error
import os
import errno

# MinIO mijozini sozlash
def initialize_minio_client():
    try:
        client = Minio(
            endpoint="minio-cdn.uzex.uz",
            access_key="cotton",
            secret_key="xV&q+8AHHXBSK}",
            secure=False  # HTTPS muammosi bo‘lsa, False qilib sinab ko‘ring
        )
        print("MinIO mijoz muvaffaqiyatli sozlandi.")
        return client
    except Exception as e:
        print(f"MinIO mijozini sozlashda xato: {str(e)}")
        return None

# Lokal papkani yaratish
def create_local_directory(local_dir):
    try:
        os.makedirs(local_dir, exist_ok=True)
        print(f"'{local_dir}' papkasi tayyor.")
        return True
    except OSError as e:
        if e.errno != errno.EEXIST:
            print(f"'{local_dir}' papkasini yaratishda xato: {str(e)}")
            return False
        return True

# Fayllarni yuklab olish
def download_files_from_minio(client, bucket_name, prefix):
    try:
        # Bucket mavjudligini tekshirish
        if not client.bucket_exists(bucket_name):
            print(f"'{bucket_name}' bucket’i mavjud emas.")
            return False
        print(f"'{bucket_name}' bucket’i topildi.")

        # Prefix bo‘yicha obyektlarni ro‘yxatlash
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        object_count = 0

        for obj in objects:
            object_name = obj.object_name
            local_file_path = object_name

            # Lokal subpapkani yaratish
            local_subdir = os.path.dirname(local_file_path)
            if local_subdir:
                create_local_directory(local_subdir)

            # Faylni yuklab olish
            print(f"'{object_name}' fayli '{local_file_path}' ga yuklanmoqda...")
            try:
                client.fget_object(bucket_name, object_name, local_file_path)
                print(f"'{object_name}' muvaffaqiyatli yuklandi.")
                object_count += 1
            except S3Error as s3_err:
                print(f"'{object_name}' faylini yuklashda xato: {str(s3_err)}")
            except Exception as e:
                print(f"'{object_name}' faylini yuklashda kutilmagan xato: {str(e)}")

        if object_count == 0:
            print(f"'{prefix}' prefix’i ostida hech qanday fayl topilmadi.")
        else:
            print(f"{object_count} ta fayl muvaffaqiyatli yuklandi.")
        return True

    except S3Error as s3_err:
        print(f"S3 xatosi yuz berdi: {str(s3_err)}")
        return False
    except Exception as e:
        print(f"Kutilmagan xato: {str(e)}")
        return False

def main():
    # Sozlamalar
    bucket_name = "cotton"
    prefix = "Templates/f794b5cf-d399-4d32-a0eb-fa4e32683551/2025/05/08/50baf5aecac149c180e797ee83aee850"

    # MinIO mijozini ishga tushirish
    client = initialize_minio_client()
    if client is None:
        print("MinIO mijozini ishga tushirish muvaffaqiyatsiz yakunlandi.")
        return

    # Fayllarni loyiha papkasiga yuklash
    if not download_files_from_minio(client, bucket_name, prefix):
        print("Fayllarni yuklash jarayoni muvaffaqiyatsiz yakunlandi.")
    else:
        print("Fayllarni yuklash jarayoni muvaffaqiyatli yakunlandi.")

if __name__ == "__main__":
    main()