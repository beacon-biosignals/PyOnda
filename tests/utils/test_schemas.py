import pyarrow as pa
import pytest
from pyonda.utils.schemas import ONDA_ANNOTATIONS_SCHEMA, ONDA_SIGNALS_SCHEMA, timespan_namedtuple


def test_timespan_namedtuple():
    TimeSpan = timespan_namedtuple(start = int(10*1e9), stop = int(20*1e9))
    assert TimeSpan._fields == ('start', 'stop')
    assert TimeSpan.start == int(10*1e9)
    assert TimeSpan.stop == int(20*1e9)


def test_timespan_namedtuple_stop_smaller_than_start():
    with pytest.raises(ValueError):
        timespan_namedtuple(start = int(20*1e9), stop = int(10*1e9))


def test_timespan_namedtuple_bad_types():
    with pytest.raises(ValueError):
        timespan_namedtuple(start = "nope", stop = int(10*1e9))

    with pytest.raises(ValueError):
        timespan_namedtuple(start = int(20*1e9), stop = "nope")

    with pytest.raises(ValueError):
        timespan_namedtuple(start = "nope", stop = "nope")

    with pytest.raises(ValueError):
        timespan_namedtuple(start = 10*1e9, stop = int(30*1e9))

    with pytest.raises(ValueError):
        timespan_namedtuple(start = int(10*1e9), stop = 30*1e9)


def test_onda_annotations_schema():
    assert ONDA_ANNOTATIONS_SCHEMA == pa.schema(
    [
        pa.field('recording', pa.binary(16), 
            nullable=False, 
            metadata={"ARROW:extension:name": "JuliaLang.UUID"}
        ),
        pa.field('id', pa.binary(16), 
            nullable=False,
            metadata={"ARROW:extension:name": "JuliaLang.UUID"}
        ),
        pa.field('span', pa.struct([('start', pa.int64()), ('stop', pa.int64())]), 
            nullable=False,
            metadata={"ARROW:extension:name": "JuliaLang.TimeSpan"}
        ),
    ]
)


def test_onda_signals_schema():
    assert ONDA_SIGNALS_SCHEMA == pa.schema(
    [
        pa.field('recording', pa.binary(16), 
            nullable=False, 
            metadata={"ARROW:extension:name": "JuliaLang.UUID"}
        ),
        pa.field('id', pa.binary(16), 
            nullable=False,
            metadata={"ARROW:extension:name": "JuliaLang.UUID"}
        ),
        pa.field('file_path', pa.string(), 
            nullable=False
        ),
        pa.field('file_format', pa.string(), 
            nullable=False
        ),
        pa.field('span', pa.struct([('start', pa.int64()), ('stop', pa.int64())]), 
            nullable=False,
            metadata={"ARROW:extension:name": "JuliaLang.TimeSpan"}
        ),
        pa.field('sensor_type', pa.string(), 
            nullable=False
        ),
        pa.field('sensor_label', pa.string(), 
            nullable=False
        ),
        pa.field('channels', pa.list_(pa.string()), 
            nullable=False
        ),
        pa.field('sample_unit', pa.string(), 
            nullable=False
        ),
        pa.field('sample_resolution_in_unit', pa.float64(), 
            nullable=False
        ),
        pa.field('sample_offset_in_unit', pa.float64(), 
            nullable=False
        ),
        pa.field('sample_type', pa.string(), 
            nullable=False
        ),
        pa.field('sample_rate', pa.float64(), 
            nullable=False
        )       
    ]
)
