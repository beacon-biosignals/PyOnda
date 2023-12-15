import pyarrow as pa
from pathlib import Path

from pyonda.utils.processing import convert_julia_uuid_bytestring_to_uuid

def test_convert_julia_uuid_bytestring_to_uuid_version4():
    """
    ```julia
    # Snippet to generate uuid4.arrow
    using Arrow, UUIDs
    row4 = (; col1=uuid4())
    table = [row4]
    Arrow.write("/path/to/tests/data/uuid4.arrow", table)

    # Snippet to generate uuid5.arrow
    namespace = uuid4()
    row5 = (; col1=uuid5(namespace, "test"))
    table = [row5]
    Arrow.write("/path/to/tests/data/uuid5.arrow", table)

    # Generate a table with both versions:
    Arrow.write("/path/to/tests/data/uuid45.arrow", [row4, row5])
    ```

    Update expected_uuid if you recreate the files
    """
    u4_file = Path(__file__).parent / 'data/uuid4.arrow'
    table4 = pa.ipc.open_file(pa.memory_map(str(u4_file), 'r')).read_all()
    df4 = table4.to_pandas()

    expected_uuid4 = "094e4b16-21cd-4acc-b5a7-e16096cdc069"
    my_uuid44 = df4['col1'].map(convert_julia_uuid_bytestring_to_uuid).iloc[0]
    assert str(my_uuid4) == expected_uuid4
    assert my_uuid4.version == 4

    u5_file = Path(__file__).parent / 'data/uuid5.arrow'
    table5 = pa.ipc.open_file(pa.memory_map(str(u5_file), 'r')).read_all()
    df5 = table5.to_pandas()

    expected_uuid5 = "0c4718ca-6f46-51a0-91c6-a0d671972447"
    my_uuid5 = df5['col1'].map(convert_julia_uuid_bytestring_to_uuid).iloc[0]
    assert str(my_uuid5) == expected_uuid5
    assert my_uuid5.version == 5

    u45_file = Path(__file__).parent / 'data/uuid45.arrow'
    table45 = pa.ipc.open_file(pa.memory_map(str(u45_file), 'r')).read_all()
    df45 = table45.to_pandas()
    my_uuids = df45['col1'].map(convert_julia_uuid_bytestring_to_uuid)
    assert str(my_uuids.iloc[0]) == expected_uuid4
    assert str(my_uuids.iloc[1]) == expected_uuid5
