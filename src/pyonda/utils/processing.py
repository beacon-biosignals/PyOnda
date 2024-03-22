import uuid
import ast
import pandas as pd
import pyarrow as pa


def convert_julia_uuid_bytestring_to_uuid(uuid_bytestring):
    """decode the UUID fields which are by default loaded as bytestrings by pandas. Due to endianness difference
    we need to reverse the bytearray before decoding the UUID to obtain the same hex string

    Parameters
    ----------
    value : bytestring
        coming from julia

    Returns
    -------
    uuid_obj: uuid.UUID
        UUID object (hex representation)
    """
    if uuid_bytestring is None:
        return None
    x = bytearray(uuid_bytestring)
    x.reverse()
    uuid_obj = uuid.UUID(bytes=bytes(x))
    return uuid_obj


def convert_python_uuid_to_uuid_bytestring(uuid_instance):
    """Convert python uuid object to a bytestring with bytes reversed. This will allow Julia to interpret the bytes as
    the correct UUID (endianness difference, cf. convert_julia_uuid)

    Parameters
    ----------
    value : uuid.UUID
        python uuid object

    Returns
    -------
    uuid_bytes: bytestring
        to be interpreted by julia
    """
    if uuid_instance is None:
        return None
    x = bytearray(uuid_instance.bytes)
    x.reverse()
    uuid_bytes = bytes(x)
    return uuid_bytes


def check_if_schema_field_has_unsupported_binary_data(field):
    """Given a pyarrow schema field, check if its type is FixedSizeBinaryType or if it is a StructType
    check if any children have the FixedSizeBinaryType in a recursive manner. Raise ValueError if we cannot
    associate the field with 'ARROW:extension:name'-'JuliaLang.UUID' key value pair metadata.
    This is done to prevent any obscure data conversions because of the endianness difference between python and julia.

    Parameters
    ----------
    field : pyarrow.Field
        schema field

    Raises
    ------
    ValueError
        FixedSizeBinaryType field has no metadata
    ValueError
        FixedSizeBinaryType field has metadata but no 'ARROW:extension:name' key
    ValueError
        FixedSizeBinaryType field has metadata with a 'ARROW:extension:name' key but not a 'JuliaLang.UUID' value
    """
    child_fields = field.flatten()
    for field in child_fields:
        if type(field.type) == pa.lib.StructType:
            check_if_schema_field_has_unsupported_binary_data(field)
        elif type(field.type) == pa.lib.FixedSizeBinaryType:
            if field.metadata is not None:
                metadata = {k.decode(): v.decode() for k, v in field.metadata.items()}
                if "ARROW:extension:name" not in metadata.keys():
                    raise ValueError(
                        f"Unsupported FixedSizeBinaryType value encountered in {field.name} (no type extension, missing ARROW:extension:name key)"
                    )
                type_extension = metadata["ARROW:extension:name"]
                if type_extension != "JuliaLang.UUID":
                    raise ValueError(
                        f"Unsupported FixedSizeBinaryType value encountered in {field.name} (unknown type extension {type_extension})"
                    )
            else:
                raise ValueError(
                    f"Unsupported FixedSizeBinaryType value encountered in {field.name} (no metadata)"
                )


def to_pandas_with_workaround_for_list_of_uuids(table, table_schema):
    # Workaround for columns that are list of binaries (like list of UUIDs originally)
    list_of_binaries_fields = []
    for schema_field in table_schema.names:
        field = table_schema.field(schema_field)
        if (
            type(field.type) == pa.ListType
            and type(field.type.value_type) == pa.FixedSizeBinaryType
        ):
            list_of_binaries_fields.append(schema_field)

    if len(list_of_binaries_fields):
        table_dict = table.to_pydict()
        for field_name in list_of_binaries_fields:
            table_dict[field_name] = [
                [str(convert_julia_uuid_bytestring_to_uuid(x)) for x in y]
                if y is not None
                else None
                for y in table_dict[field_name]
            ]

        new_table = pa.Table.from_pydict(table_dict)
        dataframe = new_table.to_pandas()
        for field_name in list_of_binaries_fields:
            dataframe[field_name] = dataframe[field_name].map(
                lambda x: [str(uuid.UUID(y)) for y in x] if x is not None else x
            )
    else:
        dataframe = table.to_pandas()
    return dataframe


def break_down_span_into_start_and_stop(dataframe, span_field):
    span_unit = span_field.type[0].type.unit
    if span_unit != "ns":
        raise ValueError("Span field is expected to contain nanosecond values")

    dataframe["start"] = dataframe["span"].map(lambda x: x["start"].seconds)
    dataframe["stop"] = dataframe["span"].map(lambda x: x["stop"].seconds)
    dataframe = dataframe.drop(["span"], axis=1)
    return dataframe


def arrow_to_processed_pandas(table):
    """Convert pyarrow table to a pandas dataframe with processing.


    Used for arrow tables generated with Onda.jl to:
    - decode the UUID fields which are by default loaded as bytestrings by pandas. Due to endianness difference
    we need to reverse the bytearray before decoding the UUID to obtain the same hex string. We do not
    convert to UUID dtype to facilitate the save/load of the pandas table as csv. The user will need to
    to apply that conversion manually on a loaded pandas dataframe if needed

    - convert timespan to two columns (start and stop), check that timespan unit is nanoseconds

    Parameters
    ----------
    table : pyarrow.Table
        input arrow table generated by Onda.jl

    Returns
    -------
    dataframe: pandas.DataFrame
        arrow table converted to processed pandas dataframe
    """
    table_schema = table.schema
    dataframe = to_pandas_with_workaround_for_list_of_uuids(table, table_schema)

    for schema_field in table_schema.names:
        field = table_schema.field(schema_field)

        # For a all columns where the value is the list, pass the type to pandas
        # When dataframe is loaded from storage, the field should be mapped with
        # ast.literal_eval to get back the list
        if type(field.type) == pa.ListType:
            dataframe[schema_field] = dataframe[schema_field].map(
                lambda x: x if x is None else list(x)
            )

        # For the span field, we will break it down into two columns
        # start / stop that will contain values in seconds
        if schema_field == "span":
            dataframe = break_down_span_into_start_and_stop(dataframe, field)

        # Check metadata for fields that are supposed to be binary typed (uuids)
        check_if_schema_field_has_unsupported_binary_data(field)
        if field.metadata is None:
            continue

        # Convert JuliaLang.UUIDs with byte reversal
        # We do this only for fields that have the following key value pair in
        # the metadata : "ARROW:extension:name" - "JuliaLang.UUID"
        metadata = {k.decode(): v.decode() for k, v in field.metadata.items()}
        if (
            "ARROW:extension:name" in metadata.keys()
            and metadata["ARROW:extension:name"] == "JuliaLang.UUID"
        ):
            dataframe[schema_field] = (
                dataframe[schema_field]
                .map(convert_julia_uuid_bytestring_to_uuid)
                .map(str)
            )

    return dataframe


def load_saved_processed_pandas(filepath):
    """We provide a utility function to load the output of
    arrow_to_processed_pandas if it was saved on disk

    We need to format some of the columns for the loaded table
    to match the output arrow_to_processed_pandas

    Parameters
    ----------
    filepath : str or Path
        path to the csv file
    """
    # If we load without dtype specification, most of the
    # column dtypes might be inferred as just object types
    df = pd.read_csv(filepath)

    # Typically if a column value contains a list, the value
    # will be read as a string by default
    def literal_eval(x):
        return ast.literal_eval(x) if not pd.isna(x) else x

    for col in df.columns:
        try:
            df[col] = df[col].map(literal_eval)
        except:
            continue

    return df
