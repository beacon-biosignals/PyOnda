import pytest
import os
import io
import numpy as np
from pathlib import Path

from pyonda.load_arrow import load_table_from_arrow_file
from pyonda.load_lpcm import (
    load_array_from_lpcm_file_buffer,
    load_array_from_lpcm_file,
    load_array_from_lpcm_zst_file
)

from tests.fixtures import (
    signal_arrow_table_path,
    lpcm_file_path,
    lpcm_zst_file_path
)


def test_load_array_from_lpcm_file_buffer(lpcm_file_path, signal_arrow_table_path):
    df = load_table_from_arrow_file(signal_arrow_table_path, processed_pandas=True)
    series = df[df['file_path'].map(lambda x : Path(x).stem == Path(lpcm_file_path).stem)].squeeze()
    
    with open(lpcm_file_path, "rb") as fh:
        buffer = io.BytesIO(fh.read())
    data = load_array_from_lpcm_file_buffer(buffer, np.dtype(series['sample_type']), len(series['channels']))
    expected_data = np.load(Path(os.path.abspath(__file__)).parent / "data/176ecfcf-d4c7-49ba-adec-f338d0a0c01f_eeg.npy")

    assert np.array_equal(data, expected_data)


def test_load_array_from_lpcm_file(lpcm_file_path, signal_arrow_table_path):
    df = load_table_from_arrow_file(signal_arrow_table_path, processed_pandas=True)
    series = df[df['file_path'].map(lambda x : Path(x).stem == Path(lpcm_file_path).stem)].squeeze()

    data = load_array_from_lpcm_file(lpcm_file_path, np.dtype(series['sample_type']), len(series['channels']))
    expected_data = np.load(Path(os.path.abspath(__file__)).parent / "data/176ecfcf-d4c7-49ba-adec-f338d0a0c01f_eeg.npy")

    assert np.array_equal(data, expected_data)


def test_load_array_from_lpcm_zst_file(lpcm_zst_file_path, signal_arrow_table_path):
    df = load_table_from_arrow_file(signal_arrow_table_path, processed_pandas=True)
    series = df[df['file_path'].map(lambda x : Path(x).stem == Path(lpcm_zst_file_path).stem)].squeeze()

    data = load_array_from_lpcm_zst_file(lpcm_zst_file_path, np.dtype(series['sample_type']), len(series['channels']))

    expected_data = np.load(Path(os.path.abspath(__file__)).parent / "data/176ecfcf-d4c7-49ba-adec-f338d0a0c01f_ecg.npy")
    assert np.array_equal(data, expected_data)
