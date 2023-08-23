import pyarrow as pa

from pyonda.utils.s3_download import download_s3_fileobj
from pyonda.utils.processing import arrow_to_processed_pandas


def load_table_from_arrow_file_buffer(buffer, processed_pandas=True):
    """Load arrow table into pyarrow table or pandas dataframe with a processing step

    Parameters
    ----------
    buffer : io.BytesIO
        Used to fill the array with data.

    processed_pandas : bool, optional
        if True apply arrow_to_processed_pandas to loaded table, by default True

    Returns
    -------
    dataframe: pandas.DataFrame
        table contents loaded into a pandas DataFrame
    """
    table = pa.ipc.open_file(buffer).read_all()
    table = arrow_to_processed_pandas(table) if processed_pandas else table
    return table


def load_table_from_arrow_file(path_to_table, processed_pandas=True):
    """Load arrow table into pyarrow table or pandas dataframe with a processing step

    Parameters
    ----------
    path_to_table : str or Path
        path to arrow table
    processed_pandas : bool, optional
        if True apply arrow_to_processed_pandas to loaded table, by default True

    Returns
    -------
    dataframe: pandas.DataFrame
        table contents loaded into a pandas DataFrame
    """
    return load_table_from_arrow_file_buffer(pa.memory_map(str(path_to_table), 'r'), processed_pandas)


def load_table_from_arrow_file_in_s3(table_url,  processed_pandas=True):
    """Load arrow table from S3 into pyarrow table or pandas dataframe with a processing step

    Parameters
    ----------
    table_url : str
        S3 URL to table
    processed_pandas : bool, optional
        if True apply arrow_to_processed_pandas to loaded table, by default True

    Returns
    -------
    dataframe: pandas.DataFrame
        table contents loaded into a pandas DataFrame
    """
    table_buf = download_s3_fileobj(table_url)
    return load_table_from_arrow_file_buffer(table_buf, processed_pandas)
