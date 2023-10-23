import pytest
import numpy as np
import os
from pathlib import Path

from pyonda.utils.decompression import decompress_zstandard_file_to_folder
from pyonda.load_arrow import load_table_from_arrow_file, load_table_from_arrow_file_in_s3
from pyonda.load_lpcm import (
    load_array_from_lpcm_file, 
    load_array_from_lpcm_file_in_s3, 
    load_array_from_lpcm_zst_file,
    load_array_from_lpcm_zst_file_in_s3
)
from pyonda.save_lpcm import (
    save_array_to_lpcm_file, 
    save_array_to_lpcm_file_in_s3,
    save_array_to_lpcm_zst_file, 
    save_array_to_lpcm_zst_file_in_s3
)

from tests.fixtures import (
    aws_credentials,
    signal_arrow_table_path,
    lpcm_file_path,
    lpcm_zst_file_path,
    s3,
    signal_arrow_table_s3_url,
    assert_signal_arrow_dataframes_equal,
    lpcm_file_s3_url
)


@pytest.fixture
def signal_table(signal_arrow_table_path):
    return load_table_from_arrow_file(signal_arrow_table_path, processed_pandas=True)


@pytest.fixture
def load_params(signal_table, lpcm_file_path):
    series = signal_table[signal_table['file_path'].map(lambda x : Path(x).stem == Path(lpcm_file_path).stem)].squeeze()
    return np.dtype(series['sample_type']), len(series['channels'])


def test_save_array_to_lpcm_file(load_params, lpcm_file_path, tmpdir):
    dtype, n_channels = load_params
    ref_array = load_array_from_lpcm_file(lpcm_file_path, dtype, n_channels)
    save_array_to_lpcm_file(ref_array, tmpdir / "test_array.lpcm")

    expected_data = np.load(Path(os.path.abspath(__file__)).parent / "data/176ecfcf-d4c7-49ba-adec-f338d0a0c01f_eeg.npy")
    saved_data = load_array_from_lpcm_file(tmpdir / "test_array.lpcm", dtype, n_channels, "C")
    assert np.array_equal(saved_data, expected_data)


def test_save_array_to_lpcm_zst_file(load_params, lpcm_file_path, tmpdir):
    dtype, n_channels = load_params
    ref_array = load_array_from_lpcm_file(lpcm_file_path, dtype, n_channels)
    save_array_to_lpcm_zst_file(ref_array, tmpdir / "test_array.lpcm.zst")

    expected_data = np.load(Path(os.path.abspath(__file__)).parent / "data/176ecfcf-d4c7-49ba-adec-f338d0a0c01f_eeg.npy")
    saved_data = load_array_from_lpcm_zst_file(tmpdir / "test_array.lpcm.zst", dtype, n_channels, "C")
    assert np.array_equal(saved_data, expected_data)

    
def test_save_table_to_s3(load_params, lpcm_file_s3_url, s3):
    dtype, n_channels = load_params
    data = load_array_from_lpcm_file_in_s3(lpcm_file_s3_url, dtype, n_channels)
    save_array_to_lpcm_file_in_s3(data, "mock-bucket", "test_array.lpcm")

    url_stem = Path(lpcm_file_s3_url).stem
    expected_data = np.load(Path(os.path.abspath(__file__)).parent / f"data/{url_stem}.npy")
    saved_data = load_array_from_lpcm_file_in_s3("s3://mock-bucket/test_array.lpcm", dtype, n_channels, "C")
    assert np.array_equal(saved_data, expected_data)


def test_save_table_to_s3_compression(load_params, lpcm_file_s3_url, s3):
    dtype, n_channels = load_params
    data = load_array_from_lpcm_file_in_s3(lpcm_file_s3_url, dtype, n_channels)
    save_array_to_lpcm_zst_file_in_s3(data, "mock-bucket", "test_array.lpcm.zst")

    url_stem = Path(lpcm_file_s3_url).stem
    expected_data = np.load(Path(os.path.abspath(__file__)).parent / f"data/{url_stem}.npy")
    saved_data = load_array_from_lpcm_zst_file_in_s3("s3://mock-bucket/test_array.lpcm.zst", dtype, n_channels, "C")
    assert np.array_equal(saved_data, expected_data)