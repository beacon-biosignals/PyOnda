import os
import pytest
import numpy as np
from pathlib import Path

from pyonda.load_arrow import load_table_from_arrow_file
from pyonda.load_lpcm import (
    load_array_from_lpcm_zst_file,
    load_array_from_lpcm_zst_file_in_s3,
)

from tests.fixtures import (
    aws_credentials,
    signal_arrow_table_path,
    lpcm_file_path,
    lpcm_zst_file_path,
    s3,
    lpcm_zst_file_s3_url,
    expected_ecg_data,
)


@pytest.fixture
def ref_series(signal_arrow_table_path, lpcm_zst_file_path):
    df = load_table_from_arrow_file(signal_arrow_table_path, processed_pandas=True)
    series = df[
        df["file_path"].map(lambda x: Path(x).stem == Path(lpcm_zst_file_path).stem)
    ].squeeze()
    return series


@pytest.fixture
def sample_type(ref_series):
    return np.dtype(ref_series["sample_type"])


@pytest.fixture
def n_channels(ref_series):
    return len(ref_series["channels"])


def test_load_array_from_lpcm_zst_file(
    lpcm_zst_file_path, sample_type, n_channels, expected_ecg_data
):
    data = load_array_from_lpcm_zst_file(lpcm_zst_file_path, sample_type, n_channels)
    assert np.array_equal(data, expected_ecg_data)


def test_load_array_from_lpcm_zst_file_in_s3(
    s3, lpcm_zst_file_s3_url, sample_type, n_channels, expected_ecg_data
):
    data = load_array_from_lpcm_zst_file_in_s3(
        lpcm_zst_file_s3_url, sample_type, n_channels
    )
    assert np.array_equal(data, expected_ecg_data)
