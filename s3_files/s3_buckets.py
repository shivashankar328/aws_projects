import boto3
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from io import StringIO
import pandas as pd
import argparse

def s3_connection(conn_key):
    """
    making a connection to an aws s3 bucket using access key
    args:
        conn_key > dict : containing the aws connection details.
                  - aws_region > str : the regin to connect
                  - aws_access_key > str : the aws access key.
                  - aws_secret_key > str : the aws secret_key.

    return: botocore.client.s3: the s3 connection established or none 

    """
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

def upload_files(s3_client, file_path, bucket_name, file_name):
    """
        uploading the files to aws s3 bucket / folder
    args: s3_client > s3 connections
          file_path > loacal file path
          bucket_name > aws s3 targeted bucket name
          file_name > user desired name to place as in s3 bucket   
    returns : file upload or error
    """

    # print('file_name', file_name)
    try:
        s3_client.upload_file(file_path, bucket_name, file_name)
        print('file is uploaded')
    except ClientError as e:
        print('file_upload_error:', e)


def get_buckets(s3_client, bucket_name, file_name):
    """ """
    try:
        res = s3_client.list_buckets(Bucket=bucket_name, key=file_name)
        #check files available in buckets
        for bucket in res['Buckets']:
            print(bucket['Name'])
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        body = response['Body'].read().decode('utf-8')
        print(f"Got object '{file_name}' from bucket '{bucket_name}'.")
        df = pd.read_csv(StringIO(body))

        return df
        
    except ClientError as e:
        print('Error retrieving data', e)

def clean_data(df):
    try:
        # remove special character for multiple columns 
        for column in ['Close/Last', 'Open', 'High', 'Low']:
            df[column] = df[column].replace('[\$,]', '', regex=True).astype(float)

        # convert date column
        df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y', errors='coerce')
        df['Date'] = df['Date'].dt.strftime('%d-%m-%Y')

        #convert dtype to volumn column
        df['Volume'] = df['Volume'].astype(int)

        return df
    except ValueError as e:
        print('clean function error:', e)
        return None


def delet_file(s3_client, bucket_name, file_name):
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)
        print(f'file {file_name} deleted from bucket {bucket_name}')
    except ClientError as e:
        print('Error occured at file deletion:', e)


def main(file_path, bucket_name, object_name, action):
    conn_key = {
                'aws_access_key': '****',
                'aws_secret_key': '****',
                'aws_s3_bucket_name':'bucket_name',
                'aws_region': 'your region'
                }

    print('object_name:', object_name)
    # making s3 connection 
    s3_client = s3_connection(conn_key)
    
    if s3_client:
        if action == 'upload':
            # upload file to s3 bucket
            upload_files(s3_client, file_path, bucket_name, object_name)

        elif action == 'delete':
            # delete file from s3 bucket
            delet_file(s3_client, bucket_name, object_name)

        elif action == 'read':
            # reading file form s3 bucket
            df = get_buckets(s3_client, bucket_name, object_name)
            df.to_csv(f'{object_name}.csv')
            ## you can write cleaning function based on specific data, dynamically
            # if df is not None:
            #     df = clean_data(df)
            #     print('final_Df:', df)
                # after cleaning you can upload above df to s3 bucket using upload function
                # upload_files(s3_clinet, df, bucket_name, 'nvidia_cleaned_file.csv')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process s3 files')
    parser.add_argument('file_path', type=str, help='path to csv file/filename or only file to upload to s3')
    parser.add_argument('bucket_name', type=str, help='targeted bucket name in s3')
    # parser.add_argument('folder_name', type=str, help='folder name inside s3 if any specific folder available')
    parser.add_argument('object_name', type=str, help='file name which you want to read or to upload in s3')
    parser.add_argument('action', type=str, choices=['upload', 'delete', 'read'], help='action to perform: upload, delete, read')

    args = parser.parse_args()

    # if folder name is available
    # if args.folder_name and args.object_name:
    #     object_name = args.folder_name.rstrip('/') + '/' + args.object_name
    # else:
    #     object_name = args.object_name

    main(args.file_path, args.bucket_name, args.object_name, args.action)