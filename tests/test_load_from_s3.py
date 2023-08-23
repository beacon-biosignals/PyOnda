import pytest 
import os
import shutil
import inspect
import boto3 

import pyarrow as pa
import numpy as np

from pathlib import Path
from moto import mock_s3

from pyonda.utils.s3_download import (
    parse_s3_url, 
    path_is_an_s3_url, 
    download_s3_file, 
    download_s3_fileobj
)

from pyonda.load_arrow import load_table_from_arrow_file_in_s3
from pyonda.load_lpcm import load_array_from_lpcm_file_in_s3, load_array_from_lpcm_zst_file_in_s3

from tests.fixtures import (
    aws_credentials,
    signal_arrow_table_path,
    lpcm_file_path,
    lpcm_zst_file_path,
    s3,
    signal_arrow_table_s3_url,
    lpcm_file_s3_url,
    lpcm_zst_file_s3_url,
    assert_signal_arrow_dataframes_equal
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


# --- Arrow load
def test_load_table_from_arrow_file_in_s3_default_is_processed():
    signature = inspect.signature(load_table_from_arrow_file_in_s3)
    defaults = {k: v.default for k, v in signature.parameters.items() if v.default is not inspect.Parameter.empty}
    assert defaults['processed_pandas']==True


def test_load_table_from_arrow_file_in_s3(s3, signal_arrow_table_s3_url, signal_arrow_table_path):
    table = load_table_from_arrow_file_in_s3(signal_arrow_table_s3_url, processed_pandas=False)
    reference_table = pa.ipc.open_file(pa.memory_map(str(signal_arrow_table_path), 'r')).read_all()
    assert table == reference_table, "Loaded arrow table is not as expected"


def test_load_table_from_arrow_file_in_s3_as_processed_dataframe(s3, signal_arrow_table_s3_url):
    df = load_table_from_arrow_file_in_s3(signal_arrow_table_s3_url, processed_pandas=True)
    assert_signal_arrow_dataframes_equal(df)


# --- LPCM load
def test_load_array_from_lpcm_file_in_s3(s3, signal_arrow_table_s3_url, lpcm_file_s3_url):
    df = load_table_from_arrow_file_in_s3(signal_arrow_table_s3_url, processed_pandas=True)
    url_stem = Path(lpcm_file_s3_url).stem
    series = df[df['file_path'].map(lambda x : Path(x).stem == url_stem)].squeeze()
    data = load_array_from_lpcm_file_in_s3(lpcm_file_s3_url, np.dtype(series['sample_type']), len(series['channels']))
    expected_data = np.load(Path(os.path.abspath(__file__)).parent / f"data/{url_stem}.npy")
    assert np.array_equal(data, expected_data)


def test_load_array_from_lpcm_zst_file_in_s3(s3, signal_arrow_table_s3_url, lpcm_zst_file_s3_url):
    df = load_table_from_arrow_file_in_s3(signal_arrow_table_s3_url, processed_pandas=True)
    url_stem = Path(lpcm_zst_file_s3_url).stem
    series = df[df['file_path'].map(lambda x : Path(x).stem == url_stem)].squeeze()
    data = load_array_from_lpcm_zst_file_in_s3(lpcm_zst_file_s3_url,np.dtype(series['sample_type']), len(series['channels']))
    expected_data = np.load(Path(os.path.abspath(__file__)).parent / f"data/{Path(url_stem).stem}.npy")
    assert np.array_equal(data, expected_data)
