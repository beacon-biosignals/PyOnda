import io
import boto3
import uuid
import zstandard
import pyarrow as pa
from pathlib import Path


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


def decompress_zstandard_file_to_folder(input_file, destination_dir):
    """Decompress .zst archive to file
    From https://stackoverflow.com/questions/55184290/how-to-decompress-lzma2-xz-and-zstd-zst-files-into-a-folder-using-python-3

    Parameters
    ----------
    input_file : str or Path
        path to .zst compressed file
    destination_dir : str or Path
        destination directory for the uncompressed file
    """
    input_file = Path(input_file)
    with open(input_file, 'rb') as compressed:
        decomp = zstandard.ZstdDecompressor()
        output_path = Path(destination_dir) / input_file.stem
        with open(output_path, 'wb') as destination:
            decomp.copy_stream(compressed, destination)


def decompress_zstandard_file_to_stream(input_file):
    """Decompress .zst archive to stream
    From https://stackoverflow.com/questions/55184290/how-to-decompress-lzma2-xz-and-zstd-zst-files-into-a-folder-using-python-3

    Parameters
    ----------
    input_file : str or Path
        path to .zst compressed file
    """
    input_file = Path(input_file)
    buf = io.BytesIO()
    with open(input_file, 'rb') as compressed:
        decomp = zstandard.ZstdDecompressor()
        decomp.copy_stream(compressed, buf)
    buf.seek(0)
    return buf


def decompress_zstandard_stream_to_file(input_stream, output_path):
    decomp = zstandard.ZstdDecompressor()
    with open(output_path, 'wb') as destination:
        decomp.copy_stream(input_stream, destination)


def decompress_zstandard_stream_to_stream(input_stream):
    buf = io.BytesIO()
    decomp = zstandard.ZstdDecompressor()
    decomp.copy_stream(input_stream, buf)
    buf.seek(0)
    return buf


def arrow_to_processed_pandas(table):
    """Convert pyarrow table to a pandas dataframe with processing
    Used for arrow tables generated with Onda.jl to:
    - decode the UUID fields which are by default loaded as bytestrings by pandas. Due to endianness difference
    we need to reverse the bytearray before decoding the UUID to obtain the same hex string
    - TODO leave timespan as a dict or convert ? 

    Parameters
    ----------
    table : pyarrow.Table
        input arrow table generated by Onda.jl

    Returns
    -------
    dataframe: pandas.DataFrame
        arrow table converted to processed pandas dataframe 
    """
    table_schema = table.schema
    dataframe = table.to_pandas()

    
    def map_uuid(x):
        x = bytearray(x)
        x.reverse()
        return uuid.UUID(bytes=bytes(x), version=4)
        
    for schema_field in table_schema.names:

        # Map UUID fields coming from Julia to UUID type
        if table_schema.field(schema_field).metadata is not None:
            metadata = {k.decode():v.decode() for k,v in table_schema.field(schema_field).metadata.items()}

            if metadata['ARROW:extension:name'] == 'JuliaLang.UUID':
                dataframe[schema_field] = dataframe[schema_field].map(map_uuid)

            if metadata['ARROW:extension:name'] == 'JuliaLang.TimeSpan':
                dataframe[f'{schema_field}_start'] = dataframe[f'{schema_field}'].map(lambda x : x['start'])
                dataframe[f'{schema_field}_stop'] = dataframe[f'{schema_field}'].map(lambda x : x['stop'])
                dataframe.drop([f'{schema_field}'], axis=1, inplace=True)


        # For a all columns where the value is the list, pass the type to pandas
        # When dataframe is loaded from storage, the field should be mapped with ast.literal_eval to get back the list
        if type(table.schema.field(schema_field).type) == pa.ListType:
            dataframe[schema_field] = dataframe[schema_field].map(list)

    return dataframe