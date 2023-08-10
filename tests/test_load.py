import pytest

import os
import ast
import uuid
import pyarrow as pa
import pandas as pd
import numpy as np
from pathlib import Path

from pyonda.load import load_arrow_table, load_arrow_table_from_s3, load_lpcm_file, load_lpcm_file_from_s3


def test_load_arrow_table():
    data_folder = Path(os.path.abspath(__file__)).parent / "data"
    path_to_table = data_folder / "test.onda.signal.arrow"

    # Test direct arrow load
    table = load_arrow_table(path_to_table, processed_pandas=False)
    assert table == pa.ipc.open_file(pa.memory_map(str(path_to_table), 'r')).read_all(), \
        "Loaded arrow table is not as expected"
    
    # Test default processed_pandas is True
    df = load_arrow_table(path_to_table, processed_pandas=True)
    pd.testing.assert_frame_equal(df, load_arrow_table(path_to_table))

    # Test processed dataframe is as expected
    df_expected = pd.read_csv(data_folder / "test.onda.signal_processed.csv")
    df_expected['channels'] = df_expected['channels'].map(ast.literal_eval)
    df_expected['recording'] = df_expected['recording'].map(uuid.UUID)
    pd.testing.assert_frame_equal(df, df_expected)


def test_load_arrow_table_from_s3_to_pandas():
    return 


def test_load_lpcm_file():
    data_folder = Path(os.path.abspath(__file__)).parent / "data"
    path_to_table = data_folder / "test.onda.signal.arrow"
    path_to_file = data_folder / "176ecfcf-d4c7-49ba-adec-f338d0a0c01f_eeg.lpcm"

    df = load_arrow_table(path_to_table, processed_pandas=True)
    series = df[df['file_path'].map(lambda x : Path(x).stem == path_to_file.stem)].squeeze()

    data = load_lpcm_file(path_to_file, np.dtype(series['sample_type']), len(series['channels']))
    expected_data = np.load(data_folder / "176ecfcf-d4c7-49ba-adec-f338d0a0c01f_eeg.npy")
    assert np.array_equal(data, expected_data)



def test_load_lpcm_file_from_s3():
    return 
