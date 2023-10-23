import boto3 
import logging

from botocore.exceptions import ClientError


def upload_file_to_s3(input_path, bucket, key):
    """Upload a file to S3
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html

    Parameters
    ----------
    input_path : str or Path
        path to file to be uploaded
    bucket : str
        destination bucket name
    key : str
        destination key
    """
    client = boto3.client("s3")
    try:
        response = client.upload_file(str(input_path), bucket, key)
    except ClientError as e:
        logging.error(e)
        return False
    return True
