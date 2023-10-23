import pyarrow as pa
import pytest
from pyonda.utils.schemas import ONDA_ANNOTATIONS_SCHEMA, timespan_namedtuple


def test_timespan_namedtuple():
    TimeSpan = timespan_namedtuple(start = int(10*1e9), stop = int(20*1e9))
    assert TimeSpan._fields == ('start', 'stop')
    assert TimeSpan.start == 10*1e9
    assert TimeSpan.stop == 20*1e9


def test_timespan_namedtuple_stop_smaller_than_start():
    with pytest.raises(ValueError):
        timespan_namedtuple(start = 20*1e9, stop = 10*1e9)


def test_timespan_namedtuple_bad_types():
    with pytest.raises(ValueError):
        timespan_namedtuple(start = "nope", stop = 10*1e9)

    with pytest.raises(ValueError):
        timespan_namedtuple(start = 20*1e9, stop = "nope")

    with pytest.raises(ValueError):
        timespan_namedtuple(start = "nope", stop = "nope")


def test_get_onda_annotations_schema():
    assert ONDA_ANNOTATIONS_SCHEMA.names == ['recording', 'id', 'span']

    recording_field = ONDA_ANNOTATIONS_SCHEMA.field('recording')
    assert recording_field.type.equals(pa.binary(16))
    assert not recording_field.nullable
    assert recording_field.metadata == {b"ARROW:extension:name": b"JuliaLang.UUID"}

    id_field = ONDA_ANNOTATIONS_SCHEMA.field('recording')
    assert id_field.type.equals(pa.binary(16))
    assert not id_field.nullable
    assert id_field.metadata == {b"ARROW:extension:name": b"JuliaLang.UUID"}
    
    span_field = ONDA_ANNOTATIONS_SCHEMA.field('span')
    assert span_field.type.equals(pa.struct([('start', pa.int64()), ('stop', pa.int64())]))
    assert not span_field.nullable
    assert span_field.metadata == {b"ARROW:extension:name": b"JuliaLang.TimeSpan"}
