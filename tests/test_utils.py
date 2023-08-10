import pytest

import os
import ast
import uuid
import pandas as pd
import pyarrow as pa
from pathlib import Path

from pyonda.utils import path_is_an_s3_url, parse_s3_url, decompress_zstandard_to_folder, arrow_to_processed_pandas


def test_path_is_an_s3_url():
    assert path_is_an_s3_url("s3://bucket/key?versionId=xxxx")
    assert not path_is_an_s3_url("/home/path/to/file")


def test_parse_s3_url():
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


def test_download_s3_file():
    return


def test_download_s3_fileobj():
    return


def test_decompress_zstandard_to_folder():
    data_folder = Path(os.path.abspath(__file__)).parent / "data"
    path_to_file = data_folder / "176ecfcf-d4c7-49ba-adec-f338d0a0c01f_ecg.lpcm.zst"
    decompress_zstandard_to_folder(path_to_file, data_folder)
    assert (data_folder / "176ecfcf-d4c7-49ba-adec-f338d0a0c01f_ecg.lpcm").is_file()
    os.remove(data_folder / "176ecfcf-d4c7-49ba-adec-f338d0a0c01f_ecg.lpcm")


def test_arrow_to_processed_pandas():
    data_folder = Path(os.path.abspath(__file__)).parent / "data"
    path_to_table = data_folder / "test.onda.signal.arrow"

    table = pa.ipc.open_file(pa.memory_map(str(path_to_table), 'r')).read_all()
    df = arrow_to_processed_pandas(table)

    df_expected = pd.read_csv(data_folder / "test.onda.signal_processed.csv")
    df_expected['channels'] = df_expected['channels'].map(ast.literal_eval)
    df_expected['recording'] = df_expected['recording'].map(uuid.UUID)
    pd.testing.assert_frame_equal(df, df_expected)
