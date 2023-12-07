import zstandard
from pathlib import Path


def compress_file_to_zst(input_file, output_dir):
    """Compress file to .zst
    https://python-zstandard.readthedocs.io/en/latest/compressor.html

    Parameters
    ----------
    input_file : str or Path
        path to original file
    output_file : str or Path
        path of output .zst file
    """
    input_file = Path(input_file)
    output_file = Path(output_dir) / f"{input_file.name}.zst"

    with open(input_file, "rb") as f:
        c = zstandard.ZstdCompressor()
        with open(output_file, "wb") as destination:
            c.copy_stream(f, destination)
