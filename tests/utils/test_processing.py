import pytest
import numpy as np
import pandas as pd

import uuid
import pyarrow as pa
from pyonda.utils.processing import (
    to_pandas_post_processed,
    convert_julia_uuid_bytestring_to_uuid,
    convert_python_uuid_to_uuid_bytestring,
    check_schema_field_has_binary_data_with_valid_metadata,
    to_pandas_extended,
)


# Test convert_julia_uuid_bytestring_to_uuid
# Test convert_python_uuid_to_uuid_bytestring
def test_uuid_conversion():

    hex_str = '77bb8f87-cea8-4ce2-896f-2aba0d253288'
    ref_uuid = uuid.UUID(hex_str)

    julia_uuid_bytestring = bytearray(ref_uuid.bytes)
    julia_uuid_bytestring.reverse()

    f1 = convert_julia_uuid_bytestring_to_uuid
    f2 = convert_python_uuid_to_uuid_bytestring

    assert f1(None) is None, "None input should return a None output"
    assert f2(None) is None, "None input should return a None output"

    assert f1(julia_uuid_bytestring) == ref_uuid
    assert f2(ref_uuid) == julia_uuid_bytestring

    assert f1(f2(ref_uuid)) == ref_uuid
    assert f2(f1(julia_uuid_bytestring)) == julia_uuid_bytestring


# Test check_if_schema_field_has_unsupported_binary_data
@pytest.fixture
def binary_fields():
    dtype = pa.binary(16)
    metadata_dict = {
        'bad_metadata' : {b"some key": b"some value"},
        'bad_metadata_key' : {b"ARROW:extension:name": b"JuliaLang.weird_UUID"},
        'valid_metadata' : {b"ARROW:extension:name": b"JuliaLang.UUID"},
    }
    fixtures = {'no_metadata': pa.field("no_metadata", dtype)}
    for key in ['bad_metadata', 'bad_metadata_key', 'valid_metadata']:
        fixtures[key] = pa.field(key, dtype, metadata=metadata_dict[key])
    return fixtures


def test_schema_field_has_supported_binary_data(binary_fields):
    check_schema_field_has_binary_data_with_valid_metadata(binary_fields['valid_metadata'])


@pytest.mark.parametrize('key', ['no_metadata', 'bad_metadata', 'bad_metadata_key'])
def test_schema_field_has_unsupported_binary_data(binary_fields, key):
    with pytest.raises(ValueError):
        check_schema_field_has_binary_data_with_valid_metadata(binary_fields[key])


def test_schema_field_is_list_with_supported_binary_data(binary_fields):
    field = pa.field("list_of_valid_binary_types", 
                     pa.list_(binary_fields['valid_metadata']))
    check_schema_field_has_binary_data_with_valid_metadata(field)

    field = pa.field("list_of_valid_binary_types", 
                     pa.list_(pa.list_(binary_fields['valid_metadata'])))
    check_schema_field_has_binary_data_with_valid_metadata(field)


@pytest.mark.parametrize('key', ['no_metadata', 'bad_metadata', 'bad_metadata_key'])
def test_schema_field_is_list_with_unsupported_binary_data(binary_fields, key):
    field = pa.field("list_of_valid_binary_types", 
                     pa.list_(binary_fields[key]))
    with pytest.raises(ValueError):
        check_schema_field_has_binary_data_with_valid_metadata(field)

    field = pa.field("list_of_valid_binary_types", 
                     pa.list_(pa.list_(binary_fields[key])))
    with pytest.raises(ValueError):
        check_schema_field_has_binary_data_with_valid_metadata(field)


# Test to_pandas_extended
def test_to_pandas_extended_no_list_of_binary_data():
    non_binary_field = pa.field("my_field", pa.string())
    my_schema = pa.schema([pa.field("list", pa.list_(non_binary_field), nullable=True)])
    my_list = [
        {"list": ["a", "b"]},
        {"list": ["c", "d"]},
    ]
    table = pa.Table.from_pylist(my_list, schema=my_schema)
    pd.testing.assert_frame_equal(to_pandas_extended(table), table.to_pandas())


def test_to_pandas_extended():
    binary_field = pa.field("bfield", pa.binary(16), metadata = {b"ARROW:extension:name": b"JuliaLang.UUID"})
    my_schema = pa.schema([pa.field("id_list", pa.list_(binary_field), nullable=True)])
    my_list = [
        {"id_list": [uuid.uuid4().bytes, uuid.uuid4().bytes]},
        {"id_list": [uuid.uuid4().bytes, uuid.uuid4().bytes]},
    ]
    table = pa.Table.from_pylist(my_list, schema=my_schema)

    # Fails with base to_pandas()
    with pytest.raises(pa.lib.ArrowNotImplementedError):
        table.to_pandas()

    # Success with extended function
    # note that processing considers bytes are coming 
    # from julia so they will be reversed
    df = to_pandas_extended(table)
    for i in range(2):
        row = df['id_list'].map(lambda x : [uuid.UUID(y) for y in x])[i]
        ref = my_list[i]['id_list']
        for j in range(2):
            assert row[j] == convert_julia_uuid_bytestring_to_uuid(ref[j])
 

# Test global processing
# TODO
def test_check_processing_supported(table):
    pass

# TODO
def test_to_pandas_post_processed(signal_arrow_table_path, reference_pandas_table):
    table = pa.ipc.open_file(
        pa.memory_map(str(signal_arrow_table_path), "r")
    ).read_all()
    df_processed = to_pandas_post_processed(table)
    pd.testing.assert_frame_equal(df_processed, reference_pandas_table)
