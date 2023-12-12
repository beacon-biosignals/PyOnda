import uuid
import pyarrow as pa


def convert_julia_uuid_bytestring_to_uuid(uuid_bytestring, version=5):
    """decode the UUID fields which are by default loaded as bytestrings by pandas. Due to endianness difference
    we need to reverse the bytearray before decoding the UUID to obtain the same hex string

    Parameters
    ----------
    value : bytestring
        coming from julia
    version: int
        uuid version (from 1 to 5)

    Returns
    -------
    uuid_obj: uuid.UUID
        UUID object (hex representation)
    """
    if uuid_bytestring is None:
        return None
    x = bytearray(uuid_bytestring)
    x.reverse()
    uuid_obj = uuid.UUID(bytes=bytes(x), version=version)
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
                metadata = {k.decode():v.decode() for k,v in field.metadata.items()}
                if 'ARROW:extension:name' not in metadata.keys():
                    raise ValueError(f'Unsupported FixedSizeBinaryType value encountered in {field.name} (no type extension, missing ARROW:extension:name key)')
                type_extension = metadata['ARROW:extension:name']
                if type_extension != 'JuliaLang.UUID':
                    raise ValueError(f'Unsupported FixedSizeBinaryType value encountered in {field.name} (unknown type extension {type_extension})')
            else:
                raise ValueError(f'Unsupported FixedSizeBinaryType value encountered in {field.name} (no metadata)')


def arrow_to_processed_pandas(table):
    """Convert pyarrow table to a pandas dataframe with processing
    Used for arrow tables generated with Onda.jl to:
    - decode the UUID fields which are by default loaded as bytestrings by pandas. Due to endianness difference
    we need to reverse the bytearray before decoding the UUID to obtain the same hex string
    - TODO leave timespan as a dict or convert ?

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
    dataframe = table.to_pandas()

    for schema_field in table_schema.names:
        field = table_schema.field(schema_field)

        # For a all columns where the value is the list, pass the type to pandas
        # When dataframe is loaded from storage, the field should be mapped with ast.literal_eval to get back the list
        if type(field.type) == pa.ListType:
            dataframe[schema_field] = dataframe[schema_field].map(lambda x: x if x is None else list(x))

        check_if_schema_field_has_unsupported_binary_data(field)

        if field.metadata is None:
            continue

        # Convert JuliaLang.UUIDs with byte reversal
        metadata = {k.decode():v.decode() for k,v in field.metadata.items()}
        if 'ARROW:extension:name' in metadata.keys() and metadata['ARROW:extension:name'] == 'JuliaLang.UUID':
            dataframe[schema_field] = dataframe[schema_field].map(convert_julia_uuid_bytestring_to_uuid)

    return dataframe

