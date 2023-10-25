import pytest
import shutil
import numpy as np

from pathlib import Path

from pyonda.utils.compression import compress_file_to_zst
from pyonda.utils.decompression import decompress_zstandard_file_to_stream

from tests.fixtures import lpcm_file_path


def test_compress_file_to_zst(tmpdir, lpcm_file_path):

    test_lpcm_file_path = Path(tmpdir) / Path(lpcm_file_path.name)
    shutil.copyfile(lpcm_file_path, test_lpcm_file_path)
    
    output_file_path = Path(tmpdir) / f"{Path(lpcm_file_path.name)}.zst"
    compress_file_to_zst(test_lpcm_file_path, tmpdir)
    assert output_file_path.is_file()

    file_buf = decompress_zstandard_file_to_stream(output_file_path)
    decompressed_data = np.ndarray(buffer = file_buf.getbuffer(), dtype = np.int16, shape = (2, 77490), order='F')

    reference_data = np.copy(np.memmap(test_lpcm_file_path, dtype = np.int16, shape = (2, 77490), order='F'))
    assert np.array_equal(decompressed_data, reference_data)
    shutil.rmtree(tmpdir)
    