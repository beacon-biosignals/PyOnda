import io
import boto3


def path_is_an_s3_url(path):
    return str(path)[:5] == "s3://"


def parse_s3_url(s3_url):
    """Parse S3 URL string to extract bucket, key and version ID

    Parameters
    ----------
    s3_url : str
        input S3 URL string

    Returns
    -------
    bucket : str
        bucket name

    key : str
        object key

    version : str
        version ID (can be None)    
    
    Raises
    ------
    ValueError
        if the input URL string does not begin with s3://
    """

    s3_url = str(s3_url)
    if not path_is_an_s3_url(s3_url):
        raise ValueError(f"Input URL is expected to start with s3://, but starts with {s3_url[:5]}")
    
    version = None
    bucket, key = s3_url.replace("s3://", "").split("/", 1)

    if '?versionId=' in key:
        key, version = key.split('?versionId=')

    return bucket, key, version


def download_s3_file(s3_url, output_file_path):
    """Given an object URL in S3, download an object from S3 to a file
    See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/download_file.html

    Parameters
    ----------
    s3_url : str or Path
        input S3 URL string 
    output_file_path : str or Path
        new path on local storage for downloaded object
    """
    client = boto3.client("s3")
    bucket, key, version = parse_s3_url(s3_url)
    if version is not None:
        client.download_file(bucket, key, output_file_path, ExtraArgs={"VersionId": version})
    else:
        client.download_file(bucket, key, output_file_path)


def download_s3_fileobj(s3_url):
    """Given an object URL in S3, download an object from S3 to a binary stream
    See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/download_fileobj.html

    Parameters
    ----------
    s3_url : str or Path
        input S3 URL string

    Returns
    -------
    buf: BytesIO
        binary stream holding the downloaded object
    """
    client = boto3.client("s3")
    bucket, key, version = parse_s3_url(s3_url)
    buf = io.BytesIO()
    if version is not None:
        client.download_fileobj(bucket, key, buf, ExtraArgs={"VersionId": version})
    else:
        client.download_fileobj(bucket, key, buf)
    buf.seek(0)
    return buf
