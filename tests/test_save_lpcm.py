import pytest
import numpy as np
from pathlib import Path

from pyonda.load_arrow import load_table_from_arrow_file
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
    
    expected_eeg_data
)

@pytest.fixture
def ref_series(signal_arrow_table_path, lpcm_file_path):
    df = load_table_from_arrow_file(signal_arrow_table_path, processed_pandas=True)
    series = df[df['file_path'].map(lambda x : Path(x).stem == Path(lpcm_file_path).stem)].squeeze()
    return series


@pytest.fixture
def sample_type(ref_series):
    return np.dtype(ref_series['sample_type'])


@pytest.fixture
def n_channels(ref_series):
    return len(ref_series['channels'])


def test_save_array_to_lpcm_file(sample_type, n_channels, expected_eeg_data, tmpdir):
    save_array_to_lpcm_file(expected_eeg_data, tmpdir / "test_array.lpcm")
    saved_data = load_array_from_lpcm_file(tmpdir / "test_array.lpcm", sample_type, n_channels, "C")
    assert np.array_equal(saved_data, expected_eeg_data)


def test_save_array_to_lpcm_zst_file(sample_type, n_channels, expected_eeg_data, tmpdir):
    save_array_to_lpcm_zst_file(expected_eeg_data, tmpdir / "test_array.lpcm.zst")
    saved_data = load_array_from_lpcm_zst_file(tmpdir / "test_array.lpcm.zst", sample_type, n_channels, "C")
    assert np.array_equal(saved_data, expected_eeg_data)

    
def test_save_array_to_lpcm_file_in_s3(s3, sample_type, n_channels, expected_eeg_data):
    save_array_to_lpcm_file_in_s3(expected_eeg_data, "mock-bucket", "test_array.lpcm")
    saved_data = load_array_from_lpcm_file_in_s3("s3://mock-bucket/test_array.lpcm", sample_type, n_channels, "C")
    assert np.array_equal(saved_data, expected_eeg_data)


def test_save_array_to_lpcm_zst_file_in_s3(s3, sample_type, n_channels, expected_eeg_data):
    save_array_to_lpcm_zst_file_in_s3(expected_eeg_data, "mock-bucket", "test_array.lpcm.zst")
    saved_data = load_array_from_lpcm_zst_file_in_s3("s3://mock-bucket/test_array.lpcm.zst", sample_type, n_channels, "C")
    assert np.array_equal(saved_data, expected_eeg_data)