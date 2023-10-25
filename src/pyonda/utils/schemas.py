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


# https://github.com/beacon-biosignals/Onda.jl/blob/main/src/annotations.jl
# cf. examples/generate_data.py
ONDA_ANNOTATIONS_SCHEMA = pa.schema(
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

# https://github.com/beacon-biosignals/Onda.jl/blob/main/src/signals.jl
ONDA_SIGNALS_SCHEMA = pa.schema(
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