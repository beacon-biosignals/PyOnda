import io
import pytest
import numpy as np
from pathlib import Path

from pyonda.load_arrow import load_table_from_arrow_file
from pyonda.load_lpcm import (
    load_array_from_lpcm_file_buffer,
    load_array_from_lpcm_file,
    load_array_from_lpcm_file_in_s3,
)


@pytest.fixture
def ref_series(signal_arrow_table_path, lpcm_file_path):
    df = load_table_from_arrow_file(signal_arrow_table_path, processed_pandas=True)
    series = df[
        df["file_path"].map(lambda x: Path(x).stem == Path(lpcm_file_path).stem)
    ].squeeze()
    return series


@pytest.fixture
def sample_type(ref_series):
    return np.dtype(ref_series["sample_type"])


@pytest.fixture
def n_channels(ref_series):
    return len(ref_series["channels"])


def test_load_array_from_lpcm_file_buffer(
    lpcm_file_path, sample_type, n_channels, expected_eeg_data
):
    with open(lpcm_file_path, "rb") as fh:
        buffer = io.BytesIO(fh.read())
    data = load_array_from_lpcm_file_buffer(buffer, sample_type, n_channels)
    assert np.array_equal(data, expected_eeg_data)


def test_load_array_from_lpcm_file(
    lpcm_file_path, sample_type, n_channels, expected_eeg_data
):
    data = load_array_from_lpcm_file(lpcm_file_path, sample_type, n_channels)
    assert np.array_equal(data, expected_eeg_data)


def test_load_array_from_lpcm_file_in_s3(
    s3, lpcm_file_s3_url, sample_type, n_channels, expected_eeg_data
):
    data = load_array_from_lpcm_file_in_s3(lpcm_file_s3_url, sample_type, n_channels)
    assert np.array_equal(data, expected_eeg_data)
