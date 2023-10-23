import os
import shutil
import pyarrow as pa
import boto3

from pyonda.utils.s3_upload import upload_file_to_s3

from tests.fixtures import (
    aws_credentials,
    signal_arrow_table_path,
    signal_arrow_table_s3_url,
    lpcm_file_path, 
    lpcm_zst_file_path,
    s3
)


def test_upload_s3_file(s3, signal_arrow_table_s3_url, signal_arrow_table_path, tmpdir):
    upload_file_to_s3(signal_arrow_table_path, "mock-bucket", "test-upload.onda.signal.arrow")

    output_file_path = os.path.join(tmpdir, 'test.onda.signal.arrow')

    client = boto3.client("s3")
    client.download_file("mock-bucket", "test-upload.onda.signal.arrow", output_file_path)

    reference_table = pa.ipc.open_file(pa.memory_map(str(signal_arrow_table_path), 'r')).read_all()
    downloaded_table = pa.ipc.open_file(pa.memory_map(output_file_path, 'r')).read_all()
    assert downloaded_table == reference_table, "Loaded arrow table is not as expected"
    
    shutil.rmtree(tmpdir)