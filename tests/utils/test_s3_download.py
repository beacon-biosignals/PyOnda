import pytest 
import os
import shutil
import pyarrow as pa

from pyonda.utils.s3_download import (
    parse_s3_url, 
    path_is_an_s3_url, 
    download_s3_file, 
    download_s3_fileobj
)

from tests.fixtures import (
    aws_credentials,
    signal_arrow_table_path,
    lpcm_file_path,
    lpcm_zst_file_path,
    s3,
    signal_arrow_table_s3_url
)


def test_path_is_an_s3_url():
    """Check that paths beginning with s3:// are considered s3 urls
    """
    assert path_is_an_s3_url("s3://bucket/key?versionId=xxxx")
    assert not path_is_an_s3_url("/home/path/to/file")


def test_parse_s3_url():
    """Test that bucket, key, version is correctly parsed from s3 urls 
    """
    with pytest.raises(ValueError):
        parse_s3_url("/home/path/to/file")
    
    url1 = "s3://bucket/very/long/and/complicated/key?versionId=xxxx"
    bucket, key, version = parse_s3_url(url1)
    assert bucket == "bucket"
    assert key == "very/long/and/complicated/key"
    assert version == "xxxx"
    
    url2 = "s3://bucket/very/long/and/complicated/key"
    bucket, key, version = parse_s3_url(url2)
    assert bucket == "bucket"
    assert key == "very/long/and/complicated/key"
    assert version is None


def test_download_s3_file(s3, signal_arrow_table_s3_url, signal_arrow_table_path, tmpdir):
    output_file_path = os.path.join(tmpdir, 'test.onda.signal.arrow')
    download_s3_file(signal_arrow_table_s3_url, output_file_path)

    reference_table = pa.ipc.open_file(pa.memory_map(str(signal_arrow_table_path), 'r')).read_all()
    downloaded_table = pa.ipc.open_file(pa.memory_map(output_file_path, 'r')).read_all()
    assert downloaded_table == reference_table, "Loaded arrow table is not as expected"
    
    shutil.rmtree(tmpdir)


def test_download_s3_fileobj(s3, signal_arrow_table_s3_url, signal_arrow_table_path):
    buf = download_s3_fileobj(signal_arrow_table_s3_url)

    reference_table = pa.ipc.open_file(pa.memory_map(str(signal_arrow_table_path), 'r')).read_all()
    downloaded_table = pa.ipc.open_file(buf).read_all()

    assert downloaded_table == reference_table, "Loaded arrow table is not as expected"
