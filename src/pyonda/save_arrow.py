import pyarrow as pa
import tempfile

from pyonda.utils.s3_upload import upload_file_to_s3
from pathlib import Path


def save_table_to_arrow_file(output_path, table, schema):
    """Sable pyarrow Table to output_path

    Parameters
    ----------
    output_path : str or Path
        output file path
    table : pyarrow.lib.Table
        table to save
    schema : pyarrow.lib.Schema
        schema of the table to save
    """
    with pa.OSFile(str(output_path), 'wb') as sink:
        with pa.ipc.new_file(sink, schema=schema) as writer:
            writer.write(table)


def save_table_to_s3(table, schema, bucket, key):
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

    Returns
    -------
    upload_status: bool
        False if ClientError is catched during upload
    """
    temp_dir = tempfile.TemporaryDirectory() 
    temp_file_path = Path(temp_dir.name) / "table_to_upload.arrow"
    save_table_to_arrow_file(temp_file_path, table, schema)
    upload_status = upload_file_to_s3(temp_file_path, bucket, key)
    temp_dir.cleanup()
    return upload_status