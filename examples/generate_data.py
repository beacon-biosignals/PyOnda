from pyonda.utils.schemas import ONDA_ANNOTATIONS_SCHEMA, timespan_namedtuple
from pyonda.utils.processing import convert_python_uuid_to_uuid_bytestring

from pyonda.save_arrow import save_table_to_arrow_file
from pyonda.save_lpcm import save_array_to_lpcm_file

from pathlib import Path

import pyarrow as pa
import numpy as np
import uuid

"""
This is a script to generate a mock Arrow table with the Annotations Schema
as well as a mock lpcm file (saving a numpy array)

The goal is to check that the data is as expected when loaded with Julia

TODO : add real test files to check this automatically
"""

original_record_uuid = uuid.uuid4()
print("Original record UUID:", original_record_uuid)

original_annot_uuids = [uuid.uuid4() for i in range(3)]
print("Original annotations UUIDs:", original_annot_uuids)

# Reverse bytes for UUIDs
rows = (
    [convert_python_uuid_to_uuid_bytestring(original_record_uuid)] * 3, 
    [convert_python_uuid_to_uuid_bytestring(x) for x in original_annot_uuids], 
    [timespan_namedtuple(start=int(n*30*(1e9)), stop=int((n+1)*30*(1e9))) for n in range(3)]
)
table = pa.Table.from_pydict(dict(zip(ONDA_ANNOTATIONS_SCHEMA.names, rows)), schema=ONDA_ANNOTATIONS_SCHEMA)

data_folder = Path(__file__).parent / 'data'
data_folder.mkdir(exist_ok=True)

save_table_to_arrow_file(table, ONDA_ANNOTATIONS_SCHEMA, data_folder / "table_test1.arrow")

# TODO : load saved table with Julia to check match

# Generate Random (10, 5) array where each line sums to 1 (probability mock)
proba_array = np.random.dirichlet(np.ones(5), size=10).astype(np.float32)
print("Mock density array: \n", proba_array)

save_array_to_lpcm_file(proba_array, data_folder / "array_test.lpcm")

""" 
# Julia code (adapt paths): 

using Arrow, TimeSpans, UUIDs, DataFrames, Dates, Onda

path_to_table = '/PyOnda/examples/data/table_test1.arrow'
table = Arrow.Table(path_to_table)
DataFrame(table)

path_to_array = '/PyOnda/examples/data/array_test.lpcm'
signal = SignalV2(;sensor_type="test", channels=["wake", "n1", "n2", "n3", "rem"], 
                   sample_unit="proba", sample_resolution_in_unit=1.0, sample_offset_in_unit=0.0, 
                   sample_type=Float32, sample_rate=1/30, recording=UUID(1), 
                   file_path=path_to_array, 
                   file_format="lpcm", span=TimeSpan(Nanosecond(0), Nanosecond(300000000000)), 
                   sensor_label="test")
Onda.load(signal)

# Onda.load shows a (5, 10) signal with channels going top down (1st row = wake)
"""