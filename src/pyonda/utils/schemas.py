import pyarrow as pa
import warnings
from collections import namedtuple


def timespan_namedtuple(start, stop):
    """https://github.com/beacon-biosignals/TimeSpans.jl
    Make sure to provide nanosecond values for start and stop

    Parameters
    ----------
    start: int
        start of span (in nanoseconds)
    stop: int
        stop of span (in nanoseconds)

    Returns
    -------
    timespan : namedtuple
        namedtuple to mimic TimeSpan struct

    Examples
    --------
    >>> TimeSpan = timespan_namedtuple()
    >>> ts = TimeSpan(start=1 * 1e9, stop=3 * 1e9)
    >>> ts
    TimeSpan(start=1000000000.0, stop=3000000000.0)
    >>> ts.start
    1000000000.0
    """
    if not isinstance(stop, int) or not isinstance(start, int):
        raise ValueError('start and stop should be integers (nanosecond values)')

    if stop <= start:
        raise ValueError('start should be < stop')

    if start != 0 and start < 1e9:
        warnings.warn('You have 0 < start < 1s, check that you put the value in nanoseconds')

    timespan = namedtuple('TimeSpan', ['start', 'stop'])
    return timespan(start=start, stop=stop)


def get_onda_annotations_schema():
    """https://github.com/beacon-biosignals/Onda.jl/blob/main/src/annotations.jl
    Return the equivalent of onda.annotation@1 schema, with metadata pointing 
    to Julia types, to make the arrow table readable with Julia

    Returns
    -------
    schema : pyarrow.schema
        pyarrow version of the annotations schema, readable by Julia

    Examples
    --------
    >>> from pyonda.utils.schemas import get_onda_annotations_schema, timespan_namedtuple
    >>> from pyonda.utils.processing import convert_python_uuid_to_uuid_bytestring
    >>> import pyarrow as pa
    >>> import uuid
    >>> schema = get_onda_annotations_schema()
    >>> record_uuid = convert_python_uuid_to_uuid_bytestring(uuid.uuid4())
    >>> rows = (
    >>>    [record_uuid] * 3, 
    >>>    [convert_python_uuid_to_uuid_bytestring(uuid.uuid4()) for i in range(3)], 
    >>>    [timespan_namedtuple(start=int(n*30*(1e9)), stop=int((n+1)*30*(1e9))) for n in range(3)]
    >>> )
    >>> table = pa.Table.from_pydict(dict(zip(schema.names, rows)), schema=schema)
    """
    return pa.schema(
        [
            pa.field('recording', pa.binary(16), 
                     metadata={"ARROW:extension:name": "JuliaLang.UUID"}),
            pa.field('id', pa.binary(16), 
                     metadata={"ARROW:extension:name": "JuliaLang.UUID"}),
            pa.field('span', pa.struct([('start', pa.int64()), ('stop', pa.int64())]), 
                     metadata={"ARROW:extension:name": "JuliaLang.TimeSpan"}),
        ]
    )