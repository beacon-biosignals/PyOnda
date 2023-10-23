import inspect
import io
import pyarrow as pa
from pathlib import Path
import numpy as np
import os
from pyonda.save_lpcm import (
    save_array_to_lpcm_file, 
    save_array_to_lpcm_file_in_s3
)

from pyonda.load_arrow import load_table_from_arrow_file, load_table_from_arrow_file_in_s3
from pyonda.load_lpcm import load_array_from_lpcm_file, load_array_from_lpcm_file_in_s3

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


def test_save_array_to_lpcm_file(signal_arrow_table_path, lpcm_file_path, tmpdir):
    df = load_table_from_arrow_file(signal_arrow_table_path, processed_pandas=True)
    series = df[df['file_path'].map(lambda x : Path(x).stem == Path(lpcm_file_path).stem)].squeeze()
    ref_array = load_array_from_lpcm_file(lpcm_file_path, np.dtype(series['sample_type']), len(series['channels']))
    save_array_to_lpcm_file(ref_array, tmpdir / "test_array.lpcm")

    expected_data = np.load(Path(os.path.abspath(__file__)).parent / "data/176ecfcf-d4c7-49ba-adec-f338d0a0c01f_eeg.npy")
    saved_data = load_array_from_lpcm_file(tmpdir / "test_array.lpcm", np.dtype(series['sample_type']), len(series['channels']), "C")
    assert np.array_equal(saved_data, expected_data)

    
def test_save_table_to_s3(signal_arrow_table_s3_url, lpcm_file_s3_url, s3):
    df = load_table_from_arrow_file_in_s3(signal_arrow_table_s3_url, processed_pandas=True)
    url_stem = Path(lpcm_file_s3_url).stem
    series = df[df['file_path'].map(lambda x : Path(x).stem == url_stem)].squeeze()
    data = load_array_from_lpcm_file_in_s3(lpcm_file_s3_url, np.dtype(series['sample_type']), len(series['channels']))

    save_array_to_lpcm_file_in_s3(data, "mock-bucket", "test_array.lpcm")

    expected_data = np.load(Path(os.path.abspath(__file__)).parent / f"data/{url_stem}.npy")
    saved_data = load_array_from_lpcm_file_in_s3("s3://mock-bucket/test_array.lpcm", np.dtype(series['sample_type']), len(series['channels']), "C")
    assert np.array_equal(saved_data, expected_data)