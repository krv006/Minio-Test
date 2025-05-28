from minio import Minio

""" This is the code for Connection to the Minio Server 
    This is The Client Information & Connection to the Minio server
    """

client = Minio(
    endpoint="minio-cdn.uzex.uz",
    access_key="cotton",
    secret_key="xV&q+8AHHXBSK}",
    secure=False,
)
""" This code will make connection to the Ninio Server with Python Now you are Ready 
To Create a Buckets & onjects """

try:
    if client.bucket_exists("image"):
        image = r'Templates/f794b5cf-d399-4d32-a0eb-fa4e32683551/2025/05/08/50baf5aecac149c180e797ee83aee850'
        destination = "information" + "/" + "myfile.jpg"
        client.fput_object(
            "image", destination, image
        )
    else:
        client.make_bucket("image")
        image = r'Templates/f794b5cf-d399-4d32-a0eb-fa4e32683551/2025/05/08/50baf5aecac149c180e797ee83aee850'
        destination = "information" + "/" + "myfile.jpg"
        client.fput_object(
            "image", destination, image
        )

except Exception as e:
    print(e)