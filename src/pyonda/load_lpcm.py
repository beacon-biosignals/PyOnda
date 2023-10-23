import numpy as np
import io

from pyonda.utils.s3_download import download_s3_fileobj
from pyonda.utils.decompression import decompress_zstandard_file_to_stream, decompress_zstandard_stream_to_stream


def load_array_from_lpcm_file_buffer(buffer, dtype, n_channels, order="F"):
    """Load lpcm file content as a numpy array with correct data type and shape

    Parameters
    ----------
    buffer : io.BytesIO
        Used to fill the array with data.
    dtype : type
        data sample type (passed to dtype argument in numpy)
    n_channels : int
        number of channels used to reshape data
    order : str
        C or F, use F to read files from Julia, use C to save files for Julia

    Returns
    -------
    data: ndarray
        numpy array with lpcm file content 
    """
    data = np.frombuffer(buffer.getbuffer(), dtype=dtype)
    full_length = len(data)
    if full_length % n_channels != 0:
        raise ValueError(f'n_channels ({n_channels}) not a multiple of array length ({full_length})')
    return data.reshape(n_channels,-1, order=order)


def load_array_from_lpcm_file(path_to_file, dtype, n_channels, order="F"):
    """Load lpcm file content as a numpy array with correct data type and shape

    Parameters
    ----------
    path_to_file : str or Path
        path to lpcm file
    dtype : type
        data sample type (passed to dtype argument in numpy.memmap)
    n_channels : int
        number of channels used to reshape data
    order : str
        C or F, use F to read files from Julia, use C to save files for Julia


    Returns
    -------
    data: ndarray
        numpy array with lpcm file content 
    """
    with open(path_to_file, "rb") as fh:
        buffer = io.BytesIO(fh.read())
    return load_array_from_lpcm_file_buffer(buffer, dtype, n_channels, order)


def load_array_from_lpcm_file_in_s3(file_url, dtype, n_channels, order="F"):
    """Load lpcm file content from S3 as a numpy array with correct data type and shape

    Parameters
    ----------
    file_url : str
        S3 URL to lpcm file
    dtype : type
        data sample type (passed to dtype argument in numpy)
    n_channels : int
        number of channels used to reshape data
    order : str
        C or F, use F to read files from Julia, use C to save files for Julia


    Returns
    -------
    data: ndarray
        numpy array with lpcm file content 
    """
    file_buf = download_s3_fileobj(file_url)
    return load_array_from_lpcm_file_buffer(file_buf, dtype, n_channels, order)


def load_array_from_lpcm_zst_file(path_to_file, dtype, n_channels):
    """Decompress lpcm zst and load file content as a numpy array with correct data type and shape

    Parameters
    ----------
    path_to_file : str or Path
        path to lpcm file
    dtype : type
        data sample type (passed to dtype argument in numpy)
    n_channels : int
        number of channels used to reshape data

    Returns
    -------
    data: ndarray
        numpy array with lpcm file content 
    """
    file_buf = decompress_zstandard_file_to_stream(path_to_file)
    return load_array_from_lpcm_file_buffer(file_buf, dtype, n_channels)


def load_array_from_lpcm_zst_file_in_s3(file_url, dtype, n_channels):
    """Decompress lpcm zst from s3 and load file content as a numpy array with correct data type and shape

    Parameters
    ----------
    path_to_file : str or Path
        path to lpcm file
    dtype : type
        data sample type (passed to dtype argument in numpy)
    n_channels : int
        number of channels used to reshape data

    Returns
    -------
    data: ndarray
        numpy array with lpcm file content 
    """
    file_buf = download_s3_fileobj(file_url)
    file_buf = decompress_zstandard_stream_to_stream(file_buf)
    return load_array_from_lpcm_file_buffer(file_buf, dtype, n_channels)
