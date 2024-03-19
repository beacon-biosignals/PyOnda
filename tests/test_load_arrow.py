import inspect
import io
import pyarrow as pa

from pyonda.load_arrow import (
    load_table_from_arrow_file_buffer,
    load_table_from_arrow_file,
    load_table_from_arrow_file_in_s3,
)
from tests.utils import assert_signal_arrow_dataframes_equal


def test_load_table_from_arrow_file_default_is_processed():
    signature = inspect.signature(load_table_from_arrow_file)
    defaults = {
        k: v.default
        for k, v in signature.parameters.items()
        if v.default is not inspect.Parameter.empty
    }
    assert defaults["processed_pandas"] == True


def test_load_table_from_arrow_file_buffer(signal_arrow_table_path):
    with open(signal_arrow_table_path, "rb") as fh:
        buffer = io.BytesIO(fh.read())

    table = load_table_from_arrow_file_buffer(buffer, processed_pandas=False)
    assert (
        table
        == pa.ipc.open_file(pa.memory_map(str(signal_arrow_table_path), "r")).read_all()
    ), "Loaded arrow table is not as expected"


def test_load_table_from_arrow_file_buffer_as_processed_dataframe(
    signal_arrow_table_path,
):
    with open(signal_arrow_table_path, "rb") as fh:
        buffer = io.BytesIO(fh.read())

    df = load_table_from_arrow_file_buffer(buffer, processed_pandas=True)
    assert_signal_arrow_dataframes_equal(df)


def test_load_table_from_arrow_file(signal_arrow_table_path):
    table = load_table_from_arrow_file(signal_arrow_table_path, processed_pandas=False)
    assert (
        table
        == pa.ipc.open_file(pa.memory_map(str(signal_arrow_table_path), "r")).read_all()
    ), "Loaded arrow table is not as expected"


def test_load_table_from_arrow_file_as_processed_dataframe(signal_arrow_table_path):
    df = load_table_from_arrow_file(signal_arrow_table_path, processed_pandas=True)
    assert_signal_arrow_dataframes_equal(df)


def test_load_table_from_arrow_file_in_s3_default_is_processed():
    signature = inspect.signature(load_table_from_arrow_file_in_s3)
    defaults = {
        k: v.default
        for k, v in signature.parameters.items()
        if v.default is not inspect.Parameter.empty
    }
    assert defaults["processed_pandas"] == True


def test_load_table_from_arrow_file_in_s3(
    s3, signal_arrow_table_s3_url, signal_arrow_table_path
):
    table = load_table_from_arrow_file_in_s3(
        signal_arrow_table_s3_url, processed_pandas=False
    )
    reference_table = pa.ipc.open_file(
        pa.memory_map(str(signal_arrow_table_path), "r")
    ).read_all()
    assert table == reference_table, "Loaded arrow table is not as expected"


def test_load_table_from_arrow_file_in_s3_as_processed_dataframe(
    s3, signal_arrow_table_s3_url
):
    df = load_table_from_arrow_file_in_s3(
        signal_arrow_table_s3_url, processed_pandas=True
    )
    assert_signal_arrow_dataframes_equal(df)
