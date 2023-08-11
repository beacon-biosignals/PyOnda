import pytest

import os
import ast
import uuid
import pyarrow as pa
import pandas as pd
import numpy as np
from pathlib import Path

from pyonda.load import (
    load_arrow_table, 
    load_arrow_table_from_s3, 
    load_lpcm_file, 
    load_lpcm_file_from_s3, 
    load_lpcm_zst_file, 
    load_lpcm_zst_file_from_s3
)


@pytest.fixture
def signal_arrow_table_path():
    return str(Path(os.path.abspath(__file__)).parent / "data/test.onda.signal.arrow")


@pytest.fixture
def signal_arrow_table_s3_url():
    return "s3://beacon-public-oss/pyonda-ci/test-data/test.onda.signal.arrow"


@pytest.fixture
def lpcm_file_path():
    return str(Path(os.path.abspath(__file__)).parent / "data/176ecfcf-d4c7-49ba-adec-f338d0a0c01f_eeg.lpcm")


@pytest.fixture
def lpcm_file_s3_url():
    return "s3://beacon-public-oss/pyonda-ci/test-data/176ecfcf-d4c7-49ba-adec-f338d0a0c01f_eeg.lpcm"


@pytest.fixture
def lpcm_zst_file_path():
    return str(Path(os.path.abspath(__file__)).parent / "data/176ecfcf-d4c7-49ba-adec-f338d0a0c01f_ecg.lpcm.zst")


@pytest.fixture
def lpcm_zst_file_s3_url():
    return "s3://beacon-public-oss/pyonda-ci/test-data/176ecfcf-d4c7-49ba-adec-f338d0a0c01f_ecg.lpcm.zst"


def test_load_arrow_table_as_pyarrow(signal_arrow_table_path):
    """Check that load_arrow_table returns the expected pyarrow table
    """
    table = load_arrow_table(signal_arrow_table_path, processed_pandas=False)
    assert table == pa.ipc.open_file(pa.memory_map(signal_arrow_table_path, 'r')).read_all(), \
        "Loaded arrow table is not as expected"


def test_load_arrow_table_default_is_processed(signal_arrow_table_path):
    """Check that load_arrow_table has processed_pandas=True by default
    """
    df = load_arrow_table(signal_arrow_table_path, processed_pandas=True)
    pd.testing.assert_frame_equal(df, load_arrow_table(signal_arrow_table_path))


def test_load_arrow_table_as_processed_dataframe(signal_arrow_table_path):
    """Check that load_arrow_table with processed_pandas=True returns the expected dataframe
    """
    df = load_arrow_table(signal_arrow_table_path, processed_pandas=True)
    df_expected = pd.read_csv(Path(signal_arrow_table_path).parent / "test.onda.signal_processed.csv")
    df_expected['channels'] = df_expected['channels'].map(ast.literal_eval)
    df_expected['recording'] = df_expected['recording'].map(uuid.UUID)
    pd.testing.assert_frame_equal(df, df_expected)


def test_load_arrow_table_from_s3_as_pyarrow(signal_arrow_table_s3_url, signal_arrow_table_path):
    """Check that load_arrow_table_from_s3 returns the expected pyarrow table
    """
    table = load_arrow_table_from_s3(signal_arrow_table_s3_url, processed_pandas=False)
    assert table == pa.ipc.open_file(pa.memory_map(signal_arrow_table_path, 'r')).read_all(), \
        "Loaded arrow table is not as expected"


def test_load_arrow_table_from_s3_default_is_processed(signal_arrow_table_s3_url):
    """Check that load_arrow_table_from_s3 has processed_pandas=True by default
    """
    df = load_arrow_table_from_s3(signal_arrow_table_s3_url, processed_pandas=True)
    pd.testing.assert_frame_equal(df, load_arrow_table_from_s3(signal_arrow_table_s3_url))


def test_load_arrow_table_from_s3_as_processed_dataframe(signal_arrow_table_s3_url, signal_arrow_table_path):
    """Check that load_arrow_table_from_s3 with processed_pandas=True returns the expected dataframe
    """
    df = load_arrow_table_from_s3(signal_arrow_table_s3_url, processed_pandas=True)
    df_expected = pd.read_csv(Path(signal_arrow_table_path).parent / "test.onda.signal_processed.csv")
    df_expected['channels'] = df_expected['channels'].map(ast.literal_eval)
    df_expected['recording'] = df_expected['recording'].map(uuid.UUID)
    pd.testing.assert_frame_equal(df, df_expected)


def test_load_lpcm_file(lpcm_file_path, signal_arrow_table_path):
    """Check that load_lpcm_file returns the expected numpy array
    """

    df = load_arrow_table(signal_arrow_table_path, processed_pandas=True)
    series = df[df['file_path'].map(lambda x : Path(x).stem == Path(lpcm_file_path).stem)].squeeze()

    data = load_lpcm_file(lpcm_file_path, np.dtype(series['sample_type']), len(series['channels']))
    expected_data = np.load(Path(os.path.abspath(__file__)).parent / "data/176ecfcf-d4c7-49ba-adec-f338d0a0c01f_eeg.npy")

    assert np.array_equal(data, expected_data)


def test_load_lpcm_file_from_s3(lpcm_file_s3_url, signal_arrow_table_path):
    """Check that load_lpcm_file_from_s3 returns the expected numpy array
    """
    df = load_arrow_table(signal_arrow_table_path, processed_pandas=True)
    series = df[df['file_path'].map(lambda x : Path(x).stem == Path(lpcm_file_s3_url).stem)].squeeze()

    data = load_lpcm_file_from_s3(lpcm_file_s3_url, np.dtype(series['sample_type']), len(series['channels']))

    expected_data = np.load(Path(os.path.abspath(__file__)).parent / "data/176ecfcf-d4c7-49ba-adec-f338d0a0c01f_eeg.npy")

    assert np.array_equal(data, expected_data)


def test_load_lpcm_zst_file(lpcm_zst_file_path, signal_arrow_table_path):
    """Check that load_lpcm_file returns the expected numpy array
    """
    df = load_arrow_table(signal_arrow_table_path, processed_pandas=True)
    series = df[df['file_path'].map(lambda x : Path(x).stem == Path(lpcm_zst_file_path).stem)].squeeze()

    data = load_lpcm_zst_file(lpcm_zst_file_path, np.dtype(series['sample_type']), len(series['channels']))

    expected_data = np.load(Path(os.path.abspath(__file__)).parent / "data/176ecfcf-d4c7-49ba-adec-f338d0a0c01f_ecg.npy")
    assert np.array_equal(data, expected_data)


def test_load_lpcm_zst_file_from_s3(lpcm_zst_file_s3_url, signal_arrow_table_path):
    """Check that load_lpcm_file returns the expected numpy array
    """
    df = load_arrow_table(signal_arrow_table_path, processed_pandas=True)
    series = df[df['file_path'].map(lambda x : Path(x).stem == Path(lpcm_zst_file_s3_url).stem)].squeeze()

    data = load_lpcm_zst_file_from_s3(lpcm_zst_file_s3_url, np.dtype(series['sample_type']), len(series['channels']))

    expected_data = np.load(Path(os.path.abspath(__file__)).parent / "data/176ecfcf-d4c7-49ba-adec-f338d0a0c01f_ecg.npy")
    assert np.array_equal(data, expected_data)
