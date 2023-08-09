import numpy as np
import pyarrow as pa

from pyonda.utils import download_s3_fileobj, arrow_to_processed_pandas


def load_arrow_table_to_pandas(path_to_table, processed=True):
    """Load arrow table into pandas dataframe with an optional processing step

    Parameters
    ----------
    path_to_table : str or Path
        path to arrow table
    processed : bool, optional
        if True apply arrow_to_processed_pandas to loaded table, by default True

    Returns
    -------
    dataframe: pandas.DataFrame
        table contents loaded into a pandas DataFrame
    """
    table = pa.ipc.open_file(pa.memory_map(path_to_table, 'r')).read_all()
    dataframe = arrow_to_processed_pandas(table) if processed else table.to_pandas()
    return dataframe


def load_arrow_table_from_s3_to_pandas(table_url,  processed=True):
    """Load arrow table from S3 into pandas dataframe with an optional processing step

    Parameters
    ----------
    table_url : str
        S3 URL to table
    processed : bool, optional
        if True apply arrow_to_processed_pandas to loaded table, by default True

    Returns
    -------
    dataframe: pandas.DataFrame
        table contents loaded into a pandas DataFrame
    """
    table_buf = download_s3_fileobj(table_url)
    table = pa.ipc.open_file(table_buf).read_all()
    dataframe = arrow_to_processed_pandas(table) if processed else table.to_pandas()
    return dataframe


def load_lpcm_file(path_to_file, sample_type, n_channels):
    """Load lpcm file content as a numpy array with correct data type and shape

    Parameters
    ----------
    path_to_file : str or Path
        path to lpcm file
    sample_type : type
        sample type needed for numpy.memmap dtype
    n_channels : int
        number of channels used to reshape data

    Returns
    -------
    data: ndarray
        numpy array with lpcm file content 
    """
    full_length = len(np.memmap(path_to_file, dtype=sample_type, mode='r'))
    data_memmap = np.memmap(path_to_file, dtype=sample_type, mode='r', shape=(n_channels, full_length // n_channels), order='F')
    data = np.copy(data_memmap)
    return data


def load_lpcm_file_from_s3(file_url, sample_type, n_channels):
    """Load lpcm file content from S3 as a numpy array with correct data type and shape

    Parameters
    ----------
    file_url : str
        S3 URL to lpcm file
    sample_type : type
        sample type needed for numpy.memmap dtype
    n_channels : int
        number of channels used to reshape data

    Returns
    -------
    data: ndarray
        numpy array with lpcm file content 
    """
    file_buf = download_s3_fileobj(file_url)
    full_length = len(np.memmap(file_buf, dtype=sample_type, mode='r'))
    data_memmap = np.memmap(file_buf, dtype=sample_type, mode='r', shape=(n_channels, full_length // n_channels), order='F')
    return np.copy(data_memmap)

    