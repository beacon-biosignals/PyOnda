import pytest
import pyarrow as pa
from pyonda.utils.processing import (
    arrow_to_processed_pandas, 
    convert_julia_uuid_bytestring_to_uuid,
    convert_python_uuid_to_uuid_bytestring,
    check_if_schema_field_has_unsupported_binary_data
)
from tests.fixtures import signal_arrow_table_path, assert_signal_arrow_dataframes_equal


def test_convert_julia_uuid():
    assert convert_julia_uuid_bytestring_to_uuid(None) is None, "None input should return a None output"
    # TODO how to test this


def test_convert_python_uuid():
    assert convert_python_uuid_to_uuid_bytestring(None) is None, "None input should return a None output"
    # TODO how to test this

    
def test_check_if_schema_field_has_unsupported_binary_data():
    # no metadata
    field = pa.field('weird_binary', pa.binary(16))
    with pytest.raises(ValueError):
        check_if_schema_field_has_unsupported_binary_data(field)

    # metadata without 'ARROW:extension:name'
    field = pa.field('weird_binary', pa.binary(16), metadata={b'some key': b'some value'})
    with pytest.raises(ValueError):
        check_if_schema_field_has_unsupported_binary_data(field)

    # metadata with 'ARROW:extension:name', unsupported value
    field = pa.field('weird_binary', pa.binary(16), metadata={b'ARROW:extension:name': b'JuliaLang.weird_UUID'})
    with pytest.raises(ValueError):
        check_if_schema_field_has_unsupported_binary_data(field)

    field = pa.field('ok_binary', pa.binary(16), metadata={b'ARROW:extension:name': b'JuliaLang.UUID'})
    check_if_schema_field_has_unsupported_binary_data(field)


def test_check_if_schema_field_has_nested_unsupported_binary_data():
    # no metadata
    struct_type = pa.struct([
        pa.field('wierd_binary_in_struct', pa.struct([
            pa.field('weird_binary', pa.binary(16)),
            pa.field('f3', pa.string()),
        ])),
        pa.field('f2', pa.string()),
    ])
    field = pa.field('wierd_binary_in_nested_struct', struct_type)

    with pytest.raises(ValueError):
        check_if_schema_field_has_unsupported_binary_data(field)

    # metadata without 'ARROW:extension:name'
    struct_type = pa.struct([
        pa.field('wierd_binary_in_struct', pa.struct([
            pa.field('weird_binary', pa.binary(16), metadata={b'some key': b'some value'}),
            pa.field('f3', pa.string()),
        ])),
        pa.field('f2', pa.string()),
    ])
    field = pa.field('wierd_binary_in_nested_struct', struct_type)
    with pytest.raises(ValueError):
        check_if_schema_field_has_unsupported_binary_data(field)

    # metadata with 'ARROW:extension:name', unsupported value
    struct_type = pa.struct([
        pa.field('wierd_binary_in_struct', pa.struct([
            pa.field('weird_binary', pa.binary(16), metadata={b'ARROW:extension:name': b'JuliaLang.weird_UUID'}),
            pa.field('f3', pa.string()),
        ])),
        pa.field('f2', pa.string()),
    ])
    field = pa.field('wierd_binary_in_nested_struct', struct_type)
    with pytest.raises(ValueError):
        check_if_schema_field_has_unsupported_binary_data(field)


    struct_type = pa.struct([
        pa.field('ok_binary_in_struct', pa.struct([
            pa.field('weird_binary', pa.binary(16), metadata={b'ARROW:extension:name': b'JuliaLang.UUID'}),
            pa.field('f3', pa.string()),
        ])),
        pa.field('f2', pa.string()),
    ])
    field = pa.field('ok_binary_in_nested_struct', struct_type)
    check_if_schema_field_has_unsupported_binary_data(field)


def test_arrow_to_processed_pandas(signal_arrow_table_path):
    table = pa.ipc.open_file(pa.memory_map(str(signal_arrow_table_path), 'r')).read_all()
    df_processed = arrow_to_processed_pandas(table)
    assert_signal_arrow_dataframes_equal(df_processed)
