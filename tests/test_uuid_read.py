import pyarrow as pa
from pathlib import Path

from pyonda.utils.processing import convert_julia_uuid_bytestring_to_uuid

def test_convert_julia_uuid_bytestring_to_uuid_version4():
    """
    ```julia
    # Snippet to generate uuid4.arrow
    using Arrow, UUIDs
    row = (; col1=uuid4())
    table = [row]
    Arrow.write("/path/to/tests/data/uuid4.arrow", table)
    ```

    Update expected_uuid if you recreate the files
    """
    u4_file = Path(__file__).parent / 'data/uuid4.arrow'
    table4 = pa.ipc.open_file(pa.memory_map(str(u4_file), 'r')).read_all()
    df4 = table4.to_pandas()

    expected_uuid = "094e4b16-21cd-4acc-b5a7-e16096cdc069"
    map_func = lambda x : str(convert_julia_uuid_bytestring_to_uuid(x))
    my_uuid = df4['col1'].map(map_func).iloc[0]
    assert my_uuid == expected_uuid
    assert my_uuid.version == 4

def test_convert_julia_uuid_bytestring_to_uuid_version5():
    """
    ```julia
    # Snippet to generate uuid5.arrow
    using Arrow, UUIDs
    namespace = uuid4()
    row = (; col1=uuid5(namespace, "test"))
    table = [row]
    Arrow.write("/path/to/tests/data/uuid5.arrow", table)
    ```

    Update expected_uuid if you recreate the files
    """
    u5_file = Path(__file__).parent / 'data/uuid5.arrow'
    table5 = pa.ipc.open_file(pa.memory_map(str(u5_file), 'r')).read_all()
    df5 = table5.to_pandas()

    expected_uuid = "0c4718ca-6f46-51a0-91c6-a0d671972447"
    map_func = lambda x : str(convert_julia_uuid_bytestring_to_uuid(x))
    my_uuid = df5['col1'].map(map_func).iloc[0]
    assert my_uuid == expected_uuid
    assert my_uuid.verions == 5
