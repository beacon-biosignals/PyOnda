import io
import zstandard
from pathlib import Path


def decompress_zstandard_file_to_folder(input_file, destination_dir):
    """Decompress .zst archive to file
    From https://stackoverflow.com/questions/55184290/how-to-decompress-lzma2-xz-and-zstd-zst-files-into-a-folder-using-python-3

    Parameters
    ----------
    input_file : str or Path
        path to .zst compressed file
    destination_dir : str or Path
        destination directory for the uncompressed file
    """
    input_file = Path(input_file)
    with open(input_file, 'rb') as compressed:
        decomp = zstandard.ZstdDecompressor()
        output_path = Path(destination_dir) / input_file.stem
        with open(output_path, 'wb') as destination:
            decomp.copy_stream(compressed, destination)


def decompress_zstandard_file_to_stream(input_file):
    """Decompress .zst archive to stream
    From https://stackoverflow.com/questions/55184290/how-to-decompress-lzma2-xz-and-zstd-zst-files-into-a-folder-using-python-3

    Parameters
    ----------
    input_file : str or Path
        path to .zst compressed file
    """
    input_file = Path(input_file)
    buf = io.BytesIO()
    with open(input_file, 'rb') as compressed:
        decomp = zstandard.ZstdDecompressor()
        decomp.copy_stream(compressed, buf)
    buf.seek(0)
    return buf


def decompress_zstandard_stream_to_file(input_stream, output_path):
    decomp = zstandard.ZstdDecompressor()
    with open(output_path, 'wb') as destination:
        decomp.copy_stream(input_stream, destination)


def decompress_zstandard_stream_to_stream(input_stream):
    buf = io.BytesIO()
    decomp = zstandard.ZstdDecompressor()
    decomp.copy_stream(input_stream, buf)
    buf.seek(0)
    return buf

