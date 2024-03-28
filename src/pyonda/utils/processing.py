import uuid
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


def check_schema_field_has_binary_data_with_valid_metadata(field):
    """Given a pyarrow schema field, check if its type is FixedSizeBinaryTypeor a list with
    FixedSizeBinaryType values in a recursive manner. Raise ValueError if we cannot
    associate the field with 'ARROW:extension:name'-'JuliaLang.UUID' key value pair metadata.
    This is done to prevent any obscure data conversions because of the endianness difference
    between python and julia.

    Examples :

    Field is a binary
      pyarrow.Field<X: fixed_size_binary[16] not null>
    It must have the desired metadata

    Field is a list
      pyarrow.Field<X: list<: fixed_size_binary[16] not null>>
    We obtain the list value type with
      schema.field("X").type.value_field -> pyarrow.Field<: fixed_size_binary[16] not null>
    It must have the desired metadata

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
        FixedSizeBinaryType field has metadata with a 'ARROW:extension:name' key but
        not a 'JuliaLang.UUID' value
    """
    if type(field.type) == pa.lib.ListType:
        value_field = field.type.value_field
        # Recursive call untill we hit the atomic element
        check_schema_field_has_binary_data_with_valid_metadata(value_field)

    elif type(field.type) == pa.lib.FixedSizeBinaryType:
        if field.metadata is not None:
            metadata = {k.decode(): v.decode() for k, v in field.metadata.items()}
            if "ARROW:extension:name" not in metadata.keys():
                raise ValueError(
                    f"Unsupported FixedSizeBinaryType value encountered in {field.name} "
                    + "(no type extension, missing ARROW:extension:name key)"
                )
            type_extension = metadata["ARROW:extension:name"]
            if type_extension != "JuliaLang.UUID":
                raise ValueError(
                    f"Unsupported FixedSizeBinaryType value encountered in {field.name} "
                    + f"(unknown type extension {type_extension})"
                )
        else:
            raise ValueError(
                f"Unsupported FixedSizeBinaryType value encountered in {field.name} "
                + "(no metadata)"
            )


def _check_processing_supported(table):
    for schema_field in table.schema.names:
        field = table.schema.field(schema_field)

        # Remove support for struct types with more than 1 level of nesting
        if type(field.type) == pa.lib.StructType:
            for child_field in field.flatten():
                if type(child_field.type) == pa.lib.StructType:
                    raise NotImplementedError(
                        "Processing struct types with more than 1 level of nesting is not supported"
                    )

                if type(child_field.type) == pa.lib.FixedSizeBinaryType:
                    raise NotImplementedError(
                        "Processing struct types with binary data is not supported"
                    )

        # Check metadata for fields that are supposed to be binary typed (uuids)
        check_schema_field_has_binary_data_with_valid_metadata(field)


def to_pandas_extended(table):
    """
    https://arrow.apache.org/docs/python/pandas.html#arrow-pandas-conversion

    The PyArrow to_pandas() function does not handle all column data types.

    If the original table contains a column whose schema is "list of binary types"
    to_pandas() will fail with a not implemented error

    This function proposes a workaround that applies the following steps:
    1. Detect columns of that type (list with FixedSizeBinaryType contents)
    2. Convert the initial table to a dictionary
    3. Convert the contents of step 1. to lists of string
       representation of the binary hex
    4. Convert the new dictionary to a pandas dataframe

    Note that all binary type data that is supposed to be UUIDs will be converted
    to string representations of the UUIDs hex in the output table.
    They can be manually cast to the UUID type if necessary with uuid.UUID(hex_str)

    Parameters
    ----------
    table : pyarrow table
        input table
    table_schema : pyarrow schema
        schema of the input table

    Returns
    -------
    dataframe: pd.DataFrame
        processed table converted to pandas format
    """
    table_schema = table.schema
    list_of_binaries_fields = []
    for field_name in table_schema.names:
        field = table_schema.field(field_name)

        if (
            type(field.type) == pa.ListType
            and type(field.type.value_type) == pa.FixedSizeBinaryType
        ):
            list_of_binaries_fields.append(field_name)

    if not len(list_of_binaries_fields):
        return table.to_pandas()

    table_dict = table.to_pydict()
    for field_name in list_of_binaries_fields:
        # Run metadata check on those fields
        field = table_schema.field(field_name)
        check_schema_field_has_binary_data_with_valid_metadata(field)

        dst_entries = []
        for src_entry in table_dict[field_name]:
            if src_entry is None:
                dst_entry = None
            else:
                dst_entry = [
                    str(convert_julia_uuid_bytestring_to_uuid(x)) for x in src_entry
                ]
            dst_entries.append(dst_entry)

        table_dict[field_name] = dst_entries
    return pa.Table.from_pydict(table_dict).to_pandas()


def to_pandas_post_processed(table):
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
    # Run checks on table to see if processing is allowed/supported
    _check_processing_supported(table)

    # First apply to_pandas_extended
    dataframe = to_pandas_extended(table)

    # Then apply post processing step
    for schema_field in table.schema.names:
        field = table.schema.field(schema_field)

        # For a all columns where the value is the list, pass the type to pandas
        # When dataframe is loaded from storage, the field should be mapped with
        # ast.literal_eval to get back the list
        if type(field.type) == pa.ListType:
            dataframe[schema_field] = dataframe[schema_field].map(
                lambda x: x if x is None else list(x)
            )

        # For the span field, we will break it down into two columns
        # start / stop that will contain values in nanoseconds
        if schema_field == "span":
            span_unit = field.type[0].type.unit
            if span_unit != "ns":
                raise ValueError("Span field is expected to contain nanosecond values")
            dataframe["start"] = dataframe["span"].map(
                lambda x: int(x["start"].total_seconds() * 1e9)
            )
            dataframe["stop"] = dataframe["span"].map(
                lambda x: int(x["stop"].total_seconds() * 1e9)
            )
            dataframe.drop(["span"], axis=1, inplace=True)

        # TODO add processing of structs with binary here if needed

        # Convert JuliaLang.UUIDs with byte reversal
        # We do this only for fields that have the following key value pair in
        # the metadata : "ARROW:extension:name" - "JuliaLang.UUID"
        if field.metadata is None:
            continue
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
