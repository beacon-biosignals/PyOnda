import shutil
import numpy as np

from pathlib import Path

from pyonda.utils.decompression import (
    decompress_zstandard_file_to_folder,
    decompress_zstandard_file_to_stream,
    decompress_zstandard_stream_to_file,
    decompress_zstandard_stream_to_stream,
)


def test_decompress_zstandard_file_to_folder(tmpdir, lpcm_zst_file_path):
    output_file_path = Path(tmpdir) / f"{lpcm_zst_file_path.stem}"
    decompress_zstandard_file_to_folder(lpcm_zst_file_path, tmpdir)
    assert output_file_path.is_file()

    decompressed_data = np.copy(np.memmap(str(output_file_path)))
    reference_data = np.copy(np.memmap(lpcm_zst_file_path.with_suffix("")))
    assert np.array_equal(decompressed_data, reference_data)

    shutil.rmtree(tmpdir)


def test_decompress_zstandard_file_to_stream(lpcm_zst_file_path):
    file_buf = decompress_zstandard_file_to_stream(lpcm_zst_file_path)

    decompressed_data = np.ndarray(
        buffer=file_buf.getbuffer(), dtype=np.int16, shape=(2, 77490), order="F"
    )

    reference_data = np.copy(
        np.memmap(
            lpcm_zst_file_path.with_suffix(""),
            dtype=np.int16,
            shape=(2, 77490),
            order="F",
        )
    )
    assert np.array_equal(decompressed_data, reference_data)


def test_decompress_zstandard_stream_to_file(tmpdir, lpcm_zst_file_path):
    output_file_path = Path(tmpdir) / f"{Path(lpcm_zst_file_path).stem}"
    with open(lpcm_zst_file_path, "rb") as f:
        decompress_zstandard_stream_to_file(f, output_file_path)

    assert output_file_path.is_file()
    decompressed_data = np.copy(np.memmap(str(output_file_path)))
    reference_data = np.copy(np.memmap(lpcm_zst_file_path.with_suffix("")))
    assert np.array_equal(decompressed_data, reference_data)

    shutil.rmtree(tmpdir)


def test_decompress_zstandard_stream_to_stream(lpcm_zst_file_path):
    with open(lpcm_zst_file_path, "rb") as f:
        file_buf = decompress_zstandard_stream_to_stream(f)
    decompressed_data = np.ndarray(
        buffer=file_buf.getbuffer(), dtype=np.int16, shape=(2, 77490), order="F"
    )

    reference_data = np.copy(
        np.memmap(
            lpcm_zst_file_path.with_suffix(""),
            dtype=np.int16,
            shape=(2, 77490),
            order="F",
        )
    )
    assert np.array_equal(decompressed_data, reference_data)
