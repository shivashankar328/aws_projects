import boto3
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from io import StringIO
import pandas as pd


def s3_connection(conn_key):
    try:
        s3_client = boto3.client(
                    service_name = 's3',
                    region_name = conn_key['aws_region'],
                    aws_access_key_id = conn_key['aws_access_key'],
                    aws_secret_access_key = conn_key['aws_secret_key']
                )
        if s3_client:
            print('connection established:')
        return s3_client
    except ClientError as e:
        print('connection_error:', e)
        return None

def upload_files(s3_clinet, file_path, bucket_name, file_name):
    # print('file_name', file_name)
    try:
        s3_clinet.upload_file(file_path, bucket_name, file_name)
        print('file is uploaded')
    except ClientError as e:
        print('file_upload_error:', e)


def get_buckets(s3_clinet, bucket_name, file_name):
    try:
        res = s3_clinet.list_buckets(Bucket=bucket_name, key=file_name)
        #check files available in buckets
        for bucket in res['Buckets']:
            print(bucket['Name'])
        response = s3_clinet.get_object(Bucket=bucket_name, Key=file_name)
        body = response['Body'].read().decode('utf-8')
        print(f"Got object '{file_name}' from bucket '{bucket_name}'.")
        df = pd.read_csv(StringIO(body))

        return df
        
    except ClientError as e:
        print('Error retrieving data', e)

def clean_data(df):
    # remove special character for multiple columns 
    for column in ['Close/Last', 'Open', 'High', 'Low']:
        df[column] = df[column].replace('[\$,]', '', regex=True).astype(float)

    # convert date column
    df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y', errors='coerce')
    df['Date'] = df['Date'].dt.strftime('%d-%m-%Y')

    #convert dtype to volumn column
    df['Volume'] = df['Volume'].astype(int)

    return df

def delet_file(s3_client, bucket_name, file_name):
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)
        print(f'file {file_name} deleted from bucket {bucket_name}')
    except ClientError as e:
        print('Error occured at file deletion:', e)

def main():
    conn_key = {
                'aws_access_key': '***',
                'aws_secret_key': '****',
                'aws_s3_bucket_name':'demo-db-328',
                'aws_region': 'us-east-2'
                }
    file_name = 'nvidia_data.csv'
    bucket_name = 'demo-db-328'
    folder_name = 'stock_details/'
    object_name = folder_name + 'nvidia_stock_price.csv'
    s3_clinet = s3_connection(conn_key)
    
    if s3_clinet:
        #upload file to s3 bucket
        upload_files(s3_clinet, file_name, bucket_name, object_name)
        
        #delete file from s3 bucket
        # file_name = folder_name +'navidia_stock_price.csv'
        # file_name = folder_name + 'stock_details.csv'

        # delet_file(s3_clinet, bucket_name, file_name)

        df = get_buckets(s3_clinet, bucket_name, object_name)
        if df is not None:
            df = clean_data(df)
            print('final_Df:', df)


if __name__ == "__main__":
    main()
