import pytest
import pandas as pd

import uuid
import pyarrow as pa
from pyonda.utils.processing import (
    arrow_to_processed_pandas,
    convert_julia_uuid_bytestring_to_uuid,
    convert_python_uuid_to_uuid_bytestring,
    check_if_schema_field_has_unsupported_binary_data,
    to_pandas_with_workaround_for_list_of_uuids,
)


def test_convert_julia_uuid():
    assert (
        convert_julia_uuid_bytestring_to_uuid(None) is None
    ), "None input should return a None output"
    # TODO how to test this


def test_convert_python_uuid():
    assert (
        convert_python_uuid_to_uuid_bytestring(None) is None
    ), "None input should return a None output"
    # TODO how to test this


def test_check_if_schema_field_has_unsupported_binary_data():
    # no metadata
    field = pa.field("weird_binary", pa.binary(16))
    with pytest.raises(ValueError):
        check_if_schema_field_has_unsupported_binary_data(field)

    # metadata without 'ARROW:extension:name'
    field = pa.field(
        "weird_binary", pa.binary(16), metadata={b"some key": b"some value"}
    )
    with pytest.raises(ValueError):
        check_if_schema_field_has_unsupported_binary_data(field)

    # metadata with 'ARROW:extension:name', unsupported value
    field = pa.field(
        "weird_binary",
        pa.binary(16),
        metadata={b"ARROW:extension:name": b"JuliaLang.weird_UUID"},
    )
    with pytest.raises(ValueError):
        check_if_schema_field_has_unsupported_binary_data(field)

    field = pa.field(
        "ok_binary",
        pa.binary(16),
        metadata={b"ARROW:extension:name": b"JuliaLang.UUID"},
    )
    check_if_schema_field_has_unsupported_binary_data(field)


def test_check_if_schema_field_has_nested_unsupported_binary_data():
    # no metadata
    struct_type = pa.struct(
        [
            pa.field(
                "wierd_binary_in_struct",
                pa.struct(
                    [
                        pa.field("weird_binary", pa.binary(16)),
                        pa.field("f3", pa.string()),
                    ]
                ),
            ),
            pa.field("f2", pa.string()),
        ]
    )
    field = pa.field("wierd_binary_in_nested_struct", struct_type)

    with pytest.raises(ValueError):
        check_if_schema_field_has_unsupported_binary_data(field)

    # metadata without 'ARROW:extension:name'
    struct_type = pa.struct(
        [
            pa.field(
                "wierd_binary_in_struct",
                pa.struct(
                    [
                        pa.field(
                            "weird_binary",
                            pa.binary(16),
                            metadata={b"some key": b"some value"},
                        ),
                        pa.field("f3", pa.string()),
                    ]
                ),
            ),
            pa.field("f2", pa.string()),
        ]
    )
    field = pa.field("wierd_binary_in_nested_struct", struct_type)
    with pytest.raises(ValueError):
        check_if_schema_field_has_unsupported_binary_data(field)

    # metadata with 'ARROW:extension:name', unsupported value
    struct_type = pa.struct(
        [
            pa.field(
                "wierd_binary_in_struct",
                pa.struct(
                    [
                        pa.field(
                            "weird_binary",
                            pa.binary(16),
                            metadata={b"ARROW:extension:name": b"JuliaLang.weird_UUID"},
                        ),
                        pa.field("f3", pa.string()),
                    ]
                ),
            ),
            pa.field("f2", pa.string()),
        ]
    )
    field = pa.field("wierd_binary_in_nested_struct", struct_type)
    with pytest.raises(ValueError):
        check_if_schema_field_has_unsupported_binary_data(field)

    struct_type = pa.struct(
        [
            pa.field(
                "ok_binary_in_struct",
                pa.struct(
                    [
                        pa.field(
                            "weird_binary",
                            pa.binary(16),
                            metadata={b"ARROW:extension:name": b"JuliaLang.UUID"},
                        ),
                        pa.field("f3", pa.string()),
                    ]
                ),
            ),
            pa.field("f2", pa.string()),
        ]
    )
    field = pa.field("ok_binary_in_nested_struct", struct_type)
    check_if_schema_field_has_unsupported_binary_data(field)


def test_arrow_to_processed_pandas(signal_arrow_table_path, reference_pandas_table):
    table = pa.ipc.open_file(
        pa.memory_map(str(signal_arrow_table_path), "r")
    ).read_all()
    df_processed = arrow_to_processed_pandas(table)
    pd.testing.assert_frame_equal(df_processed, reference_pandas_table)


def test_arrow_has_col_with_list_of_binary_types():
    # A list of binaries cant be converted easily to pandas with pyarrow
    my_schema = pa.schema([pa.field("id_list", pa.list_(pa.binary(16)), nullable=True)])
    for schema_field in my_schema.names:
        field = my_schema.field(schema_field)
        if (
            type(field.type) == pa.ListType
            and type(field.type.value_type) == pa.FixedSizeBinaryType
        ):
            print("ok")

    my_list = [
        {"id_list": [uuid.uuid4().bytes, uuid.uuid4().bytes]},
        {"id_list": [uuid.uuid4().bytes, uuid.uuid4().bytes]},
    ]
    table = pa.Table.from_pylist(my_list, schema=my_schema)

    with pytest.raises(pa.lib.ArrowNotImplementedError):
        table.to_pandas()

    # Workaround
    df = to_pandas_with_workaround_for_list_of_uuids(table, my_schema)
