import pytest

import os
import ast
import uuid
import shutil
import pandas as pd
import pyarrow as pa
import numpy as np
from pathlib import Path


from pyonda.utils import (
    path_is_an_s3_url, 
    parse_s3_url, 
    decompress_zstandard_file_to_folder, 
    decompress_zstandard_file_to_stream,
    decompress_zstandard_stream_to_file,
    decompress_zstandard_stream_to_stream,
    arrow_to_processed_pandas, 
    download_s3_file, 
    download_s3_fileobj
)


@pytest.fixture
def signal_arrow_table_s3_url():
    return f"s3://beacon-public-oss/pyonda-ci/test-data/test.onda.signal.arrow"

@pytest.fixture
def signal_arrow_table_path():
    return str(Path(os.path.abspath(__file__)).parent / "data/test.onda.signal.arrow")


@pytest.fixture
def lpcm_zst_file_path():
    return str(Path(os.path.abspath(__file__)).parent / "data/176ecfcf-d4c7-49ba-adec-f338d0a0c01f_ecg.lpcm.zst")


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


def test_download_s3_file(signal_arrow_table_s3_url, signal_arrow_table_path, tmpdir):
    output_file_path = os.path.join(tmpdir, 'test.onda.signal.arrow')

    download_s3_file(signal_arrow_table_s3_url, output_file_path)

    table = pa.ipc.open_file(pa.memory_map(output_file_path, 'r')).read_all()
    assert table == pa.ipc.open_file(pa.memory_map(signal_arrow_table_path, 'r')).read_all(), \
        "Loaded arrow table is not as expected"
    
    shutil.rmtree(tmpdir)


def test_download_s3_fileobj(signal_arrow_table_s3_url, signal_arrow_table_path):
    buf = download_s3_fileobj(signal_arrow_table_s3_url)
    table = pa.ipc.open_file(buf).read_all()

    assert table == pa.ipc.open_file(pa.memory_map(signal_arrow_table_path, 'r')).read_all(), \
        "Loaded arrow table is not as expected"


def test_decompress_zstandard_file_to_folder(tmpdir, lpcm_zst_file_path):
    output_file_path = Path(tmpdir) / "176ecfcf-d4c7-49ba-adec-f338d0a0c01f_ecg.lpcm"
    decompress_zstandard_file_to_folder(lpcm_zst_file_path, tmpdir)
    assert output_file_path.is_file()

    decompressed_data = np.copy(np.memmap(str(output_file_path)))
    reference_data = np.copy(np.memmap(lpcm_zst_file_path.replace('.zst', '')))
    assert np.array_equal(decompressed_data, reference_data)

    shutil.rmtree(tmpdir)


def test_decompress_zstandard_file_to_stream(lpcm_zst_file_path):
    file_buf = decompress_zstandard_file_to_stream(lpcm_zst_file_path)

    decompressed_data = np.ndarray(buffer = file_buf.getbuffer(), dtype = np.int16, shape = (19, 30720), order='F')
    reference_data = np.copy(np.memmap(lpcm_zst_file_path.replace('.zst', '')))
    assert np.array_equal(decompressed_data, reference_data)


def test_decompress_zstandard_stream_to_file(tmpdir, lpcm_zst_file_path):
    output_file_path = Path(tmpdir) / "176ecfcf-d4c7-49ba-adec-f338d0a0c01f_ecg.lpcm"
    with open(lpcm_zst_file_path, 'rb') as f:
        decompress_zstandard_stream_to_file(f)

    assert output_file_path.is_file()
    decompressed_data = np.copy(np.memmap(str(output_file_path)))
    reference_data = np.copy(np.memmap(lpcm_zst_file_path.replace('.zst', '')))
    assert np.array_equal(decompressed_data, reference_data)

    shutil.rmtree(tmpdir)


def test_decompress_zstandard_stream_to_stream():
    with open(lpcm_zst_file_path, 'rb') as f:
        file_buf = decompress_zstandard_stream_to_stream(f)
    decompressed_data = np.ndarray(buffer = file_buf.getbuffer(), dtype = np.int16, shape = (19, 30720), order='F')
    reference_data = np.copy(np.memmap(lpcm_zst_file_path.replace('.zst', '')))
    assert np.array_equal(decompressed_data, reference_data)


def test_arrow_to_processed_pandas(signal_arrow_table_path):
    table = pa.ipc.open_file(pa.memory_map(str(signal_arrow_table_path), 'r')).read_all()
    df = arrow_to_processed_pandas(table)

    df_expected = pd.read_csv(Path(signal_arrow_table_path).parent / "test.onda.signal_processed.csv")
    df_expected['channels'] = df_expected['channels'].map(ast.literal_eval)
    df_expected['recording'] = df_expected['recording'].map(uuid.UUID)
    pd.testing.assert_frame_equal(df, df_expected)
