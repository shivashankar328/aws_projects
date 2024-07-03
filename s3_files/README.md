
# S3 connections using python

For S3 connections and files operations like upload, delete, read


# requirements

    boto3
    pandas
    argparser

# s3 connections requirements
    access_key
    secret_key
    bucket_name
    region_name

# usecase
    python script_name.py path/to/your/local/file.csv your-s3-bucket-name your-object-name.csv upload
    python script_name.py path/to/your/local/file.csv your-s3-bucket-name your-object-name.csv delete
    python script_name.py path/to/your/local/file.csv your-s3-bucket-name your-object-name.csv read