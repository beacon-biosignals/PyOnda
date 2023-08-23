
import pytest
import inspect
import io
import pyarrow as pa


from pyonda.load_arrow import load_table_from_arrow_file_buffer, load_table_from_arrow_file
from pyonda.utils.processing import (
    arrow_to_processed_pandas, 
    convert_julia_uuid,
    check_if_schema_field_has_unsupported_binary_data
)

from tests.fixtures import (
    signal_arrow_table_path, 
    lpcm_zst_file_path,
    assert_signal_arrow_dataframes_equal
)


def test_convert_julia_uuid():
    assert convert_julia_uuid(None) is None, "None input should return a None output"
    # TODO how to test this

    
def test_check_if_schema_field_has_unsupported_binary_data():
    field = pa.field('weird_binary', pa.binary(16))
    with pytest.raises(ValueError):
        check_if_schema_field_has_unsupported_binary_data(field)

    field = pa.field('weird_binary', pa.binary(16), metadata={b'ARROW:extension:name': b'JuliaLang.weird_UUID'})
    with pytest.raises(ValueError):
        check_if_schema_field_has_unsupported_binary_data(field)

    field = pa.field('weird_binary', pa.binary(16), metadata={b'ARROW:extension:name': b'JuliaLang.UUID'})
    check_if_schema_field_has_unsupported_binary_data(field)


def test_arrow_to_processed_pandas(signal_arrow_table_path):
    table = pa.ipc.open_file(pa.memory_map(str(signal_arrow_table_path), 'r')).read_all()
    df_processed = arrow_to_processed_pandas(table)
    assert_signal_arrow_dataframes_equal(df_processed)


def test_load_table_from_arrow_file_default_is_processed():
    signature = inspect.signature(load_table_from_arrow_file)
    defaults = {k: v.default for k, v in signature.parameters.items() if v.default is not inspect.Parameter.empty}
    assert defaults['processed_pandas']==True


def test_load_table_from_arrow_file_buffer(signal_arrow_table_path):
    with open(signal_arrow_table_path, "rb") as fh:
        buffer = io.BytesIO(fh.read())

    table = load_table_from_arrow_file_buffer(buffer, processed_pandas=False)
    assert table == pa.ipc.open_file(pa.memory_map(str(signal_arrow_table_path), 'r')).read_all(), \
        "Loaded arrow table is not as expected"


def test_load_table_from_arrow_file_buffer_as_processed_dataframe(signal_arrow_table_path):
    with open(signal_arrow_table_path, "rb") as fh:
        buffer = io.BytesIO(fh.read())

    df = load_table_from_arrow_file_buffer(buffer, processed_pandas=True)
    assert_signal_arrow_dataframes_equal(df)


def test_load_table_from_arrow_file(signal_arrow_table_path):
    table = load_table_from_arrow_file(signal_arrow_table_path, processed_pandas=False)
    assert table == pa.ipc.open_file(pa.memory_map(str(signal_arrow_table_path), 'r')).read_all(), \
        "Loaded arrow table is not as expected"


def test_load_table_from_arrow_file_as_processed_dataframe(signal_arrow_table_path):
    df = load_table_from_arrow_file(signal_arrow_table_path, processed_pandas=True)
    assert_signal_arrow_dataframes_equal(df)
