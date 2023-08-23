import pytest
import os
import pandas as pd
import ast
import uuid
import boto3 
from pathlib import Path
from moto import mock_s3

@pytest.fixture
def lpcm_zst_file_path():
    return Path(os.path.abspath(__file__)).parent / "data/176ecfcf-d4c7-49ba-adec-f338d0a0c01f_ecg.lpcm.zst"


@pytest.fixture
def signal_arrow_table_path():
    return Path(os.path.abspath(__file__)).parent / "data/test.onda.signal.arrow"


def assert_signal_arrow_dataframes_equal(df_processed):
    df_expected = pd.read_csv(Path(os.path.abspath(__file__)).parent / "data/test.onda.signal_processed.csv")
    df_expected['channels'] = df_expected['channels'].map(ast.literal_eval)
    df_expected['recording'] = df_expected['recording'].map(uuid.UUID)
    df_expected['span']= df_expected['span'].map(ast.literal_eval)

    for df in [df_processed, df_expected]:
        df['start_span'] = df['span'].map(lambda x : int(x['start']))
        df['stop_span'] = df['span'].map(lambda x : int(x['stop']))
        df.drop(['span'], axis=1, inplace=True)

    pd.testing.assert_frame_equal(df, df_expected)


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-2"


@pytest.fixture
def lpcm_file_path():
    return Path(os.path.abspath(__file__)).parent / "data/176ecfcf-d4c7-49ba-adec-f338d0a0c01f_eeg.lpcm"


@pytest.fixture
def signal_arrow_table_s3_url():
    return "s3://mock-bucket/test.onda.signal.arrow"


@pytest.fixture
def lpcm_file_s3_url():
    return "s3://mock-bucket/176ecfcf-d4c7-49ba-adec-f338d0a0c01f_eeg.lpcm"


@pytest.fixture
def lpcm_zst_file_s3_url():
    return "s3://mock-bucket/176ecfcf-d4c7-49ba-adec-f338d0a0c01f_ecg.lpcm.zst"


@pytest.fixture(scope="function")
def s3(aws_credentials, signal_arrow_table_path, lpcm_file_path, lpcm_zst_file_path):
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="mock-bucket")
        s3.upload_file(signal_arrow_table_path, "mock-bucket", "test.onda.signal.arrow")
        s3.upload_file(lpcm_file_path, "mock-bucket", "176ecfcf-d4c7-49ba-adec-f338d0a0c01f_eeg.lpcm")
        s3.upload_file(lpcm_zst_file_path, "mock-bucket", "176ecfcf-d4c7-49ba-adec-f338d0a0c01f_ecg.lpcm.zst")
        yield s3