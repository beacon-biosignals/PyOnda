import pandas as pd


import ast
import os
import uuid
from pathlib import Path


def assert_signal_arrow_dataframes_equal(df_processed):
    """Load data/test.onda.signal_processed.csv as df, add some preprocessings to ensure
    both dataframes are preprocessed similarly and apply pandas.testing.assert_frame_equal

    Parameters
    ----------
    df_processed : pandas.DataFrame
        input dataframe to be tested
    """
    df_expected = pd.read_csv(
        Path(os.path.abspath(__file__)).parent / "data/test.onda.signal_processed.csv"
    )
    df_expected["channels"] = df_expected["channels"].map(ast.literal_eval)
    df_expected["recording"] = df_expected["recording"].map(uuid.UUID)
    pd.testing.assert_frame_equal(df_processed, df_expected)
