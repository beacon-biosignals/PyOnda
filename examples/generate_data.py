from pyonda.utils.schemas import (
    ONDA_ANNOTATIONS_SCHEMA,
    ONDA_SIGNALS_SCHEMA,
    timespan_namedtuple,
)
from pyonda.utils.processing import convert_python_uuid_to_uuid_bytestring

from pyonda.save_arrow import save_table_to_arrow_file
from pyonda.save_lpcm import save_array_to_lpcm_file

from pathlib import Path

import pyarrow as pa
import numpy as np
import uuid
import os

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
rows = {
    "recording": [convert_python_uuid_to_uuid_bytestring(original_record_uuid)] * 3,
    "id": [convert_python_uuid_to_uuid_bytestring(x) for x in original_annot_uuids],
    "span": [
        timespan_namedtuple(start=int(n * 30 * (1e9)), stop=int((n + 1) * 30 * (1e9)))
        for n in range(3)
    ],
}
table = pa.Table.from_pydict(rows, schema=ONDA_ANNOTATIONS_SCHEMA)

data_folder = Path(__file__).parent.resolve() / "data"
data_folder.mkdir(exist_ok=True)
print(f"Saving to {data_folder}")

save_table_to_arrow_file(
    table, ONDA_ANNOTATIONS_SCHEMA, data_folder / "table_test.annotations.arrow"
)

# TODO : load saved table with Julia to check match

# Generate Random (10, 5) array where each line sums to 1 (probability mock)
proba_array = np.random.dirichlet(np.ones(5), size=10).astype(np.float32)
print("Mock density array: \n", proba_array)

save_array_to_lpcm_file(proba_array, data_folder / "array_test.lpcm")

# Save signal table
original_signal_uuid = uuid.uuid4()
print(str(data_folder / "array_test.lpcm"))
rows = {
    "recording": [convert_python_uuid_to_uuid_bytestring(original_record_uuid)],
    "id": [convert_python_uuid_to_uuid_bytestring(original_signal_uuid)],
    "file_path": [str(data_folder / "array_test.lpcm")],
    "file_format": ["lpcm"],
    "span": [timespan_namedtuple(start=0, stop=int(300 * (1e9)))],
    "sensor_type": ["segmentation_model"],
    "sensor_label": ["hypnodensity"],
    "channels": [["wake", "n1", "n2", "n3", "rem"]],
    "sample_unit": ["probability"],
    "sample_resolution_in_unit": [1],
    "sample_offset_in_unit": [0],
    "sample_type": ["float32"],
    "sample_rate": [1 / 30],
}
table = pa.Table.from_pydict(rows, schema=ONDA_SIGNALS_SCHEMA)
save_table_to_arrow_file(
    table, ONDA_SIGNALS_SCHEMA, data_folder / "table_test.signal.arrow"
)
