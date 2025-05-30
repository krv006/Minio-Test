import os
import boto3
from botocore.client import Config

s3 = boto3.resource(
    's3',
    endpoint_url='https://minio-cdn.uzex.uz',
    aws_access_key_id='cotton',
    aws_secret_access_key='xV&q+8AHHXBSK}',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

bucket_name = 'cotton'
object_key = 'Records/ca84910d-470a-4ac6-0e66-08dd96cf2567/2025.05.20/ceb4bc7eafcb4092a03eb37899dd9fbf'
local_path = 'krv_test.xlsx'

s3.Bucket(bucket_name).download_file(object_key, local_path)
print(f"Fayl '{object_key}' muvaffaqiyatli yuklab olindi va '{local_path}' ga saqlandi.")
