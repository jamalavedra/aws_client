import argparse
import logging
import boto3
from botocore.exceptions import ClientError
import re
import os

def init_client(keys_path):
    """Initialize client."""
    client = None
    access_key = ''
    secret_key = ''

    if keys_path:
        with open(keys_path, 'r') as fkeys:
            keys = fkeys.read()
            try:
                access_key = re.search(r'AccessKeyId[=:]([^\n\r]+)', keys, re.I).group(1)
                secret_key = re.search(r'SecretKey[=:]([^\n\r]+)', keys, re.I).group(1)
            except AttributeError:
                raise ValueError('Credentials are not in the right format. '
                                 'The expected format is:\nAccessKeyId=XXXX\nSecretKey=XXXX')

    client = boto3.client('s3', aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key)
    return client

def upload_s3_bucket(file_name, bucket, client, object_name = None):

    if object_name is None:
        object_name = file_name
    # Upload the file
    try:
        response = client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def download_s3_bucket(bucket, object_name, client, directory = None):
    # Upload the file
    try:
        response = client.download_file(bucket, object_name, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    if directory is not None:
        current_directory = os.path.dirname(os.path.abspath(__file__)) + '/' + object_name
        os.rename(current_directory, directory)
    return True

def list_s3_bucket(client):
    # Retrieve the list of existing buckets
    try:
        response = client.list_buckets()
    except ClientError as e:
        logging.error(e)
        return False
    # Output the bucket names
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')
    return True

def list_s3_objects_bucket(bucket_name: 'str', client):
    # Retrieve the list of existing buckets
    try:
        response = client.list_objects(Bucket = bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    # Output the bucket names
    print(f'Existing objects in {bucket_name}:')
    for bucket in response['Contents']:
            print(f'  {bucket["Key"]}')
    return True

def main():
    """Execute main code."""
    parser = argparse.ArgumentParser()
    exclusive_args = parser.add_mutually_exclusive_group()
    exclusive_args.add_argument('-u', '--upload',
                                help='the path of the file to be uploaded')
    exclusive_args.add_argument('-d', '--download',
                                help='path to the bucket containing the file')
    parser.add_argument('-b', '--blist', action='store_true',
                        help='bucket list')
    parser.add_argument('-o', '--olist',
                        help='path to the bucket to list')
    parser.add_argument('-f', '--filepath',
                        help='path to the file')
    parser.add_argument('-l', '--location',
                        help='location of the file')
    required_args = parser.add_argument_group('required arguments')
    required_args.add_argument('-k', '--keys', required=True,
                        help='the path of your credentials file')
    args = parser.parse_args()

    client = init_client(args.keys)

    if args.upload:
        upload_s3_bucket(args.upload, args.filepath, client)
    if args.download:
        download_s3_bucket(args.download, args.filepath, client, args.location)
    if args.blist:
        list_s3_bucket(client)
    if args.olist:
        list_s3_objects_bucket(args.olist, client)

if __name__ == '__main__':
    main()

    '''
    keys.csv
    AWSAccessKeyId=XXXX
    AWSSecretKey=XXXX
    '''