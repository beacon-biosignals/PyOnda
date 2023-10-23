import pyarrow as pa
import pytest
from pyonda.utils.schemas import get_onda_annotations_schema, timespan_namedtuple


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
    schema = get_onda_annotations_schema()
    assert schema.names == ['recording', 'id', 'span']
    assert get_onda_annotations_schema().field('recording').type.equals(pa.binary(16))
    assert get_onda_annotations_schema().field('id').type.equals(pa.binary(16))
    assert get_onda_annotations_schema().field('span').type.equals(pa.struct([('start', pa.int64()), ('stop', pa.int64())]))
