import pyarrow as pa
import tempfile

from pyonda.utils.s3_upload import upload_file_to_s3
from pathlib import Path
from botocore.client import BaseClient


def save_table_to_arrow_file(table, schema, output_path):
    """Sable pyarrow Table to output_path

    Parameters
    ----------
    table : pyarrow.lib.Table
        table to save
    schema : pyarrow.lib.Schema
        schema of the table to save
    output_path : str or Path
        output file path
    """
    with pa.OSFile(str(output_path), 'wb') as sink:
        with pa.ipc.new_file(sink, schema=schema) as writer:
            writer.write(table)


def save_table_to_s3(table, schema, bucket, key, client:BaseClient=None):
    """Save table in a temp dir and upload it to s3

    Parameters
    ----------
    table : pyarrow.lib.Table
        table to save
    schema : pyarrow.lib.Schema
        schema of the table to save
    bucket : str
        destination bucket name
    key : str
        destination file key
    client: BaseClient, default=None
        boto3 client instance

    Returns
    -------
    upload_status: bool
        False if ClientError is catched during upload
    """
    temp_dir = tempfile.TemporaryDirectory() 
    temp_file_path = Path(temp_dir.name) / "table_to_upload.arrow"
    try:
        save_table_to_arrow_file(table, schema, temp_file_path)
        upload_file_to_s3(temp_file_path, bucket, key, client)
    finally:
        temp_dir.cleanup()
