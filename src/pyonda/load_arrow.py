import pyarrow as pa

from pyonda.utils.s3_download import download_s3_fileobj
from pyonda.utils.processing import to_pandas_post_processed

from botocore.client import BaseClient


def load_table_from_arrow_file_buffer(buffer, processed_pandas=True):
    """Load arrow table into pyarrow table or pandas dataframe with a processing step
    Note that UUID content will be converted to string representations if you convert
    the table to pandas.

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
    table = to_pandas_post_processed(table) if processed_pandas else table
    return table


def load_table_from_arrow_file(path_to_table, processed_pandas=True):
    """Load arrow table into pyarrow table or pandas dataframe with a processing step
    Note that UUID content will be converted to string representations if you convert
    the table to pandas.

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
    return load_table_from_arrow_file_buffer(
        pa.memory_map(str(path_to_table), "r"), processed_pandas
    )


def load_table_from_arrow_file_in_s3(
    table_url, processed_pandas=True, client: BaseClient = None
):
    """Load arrow table from S3 into pyarrow table or pandas dataframe with a processing step
    Note that UUID content will be converted to string representations if you convert
    the table to pandas.

    Parameters
    ----------
    table_url : str
        S3 URL to table
    processed_pandas : bool, optional
        if True apply arrow_to_processed_pandas to loaded table, by default True
    client: BaseClient, default=None
        boto3 client instance

    Returns
    -------
    dataframe: pandas.DataFrame
        table contents loaded into a pandas DataFrame
    """
    table_buf = download_s3_fileobj(table_url, client)
    return load_table_from_arrow_file_buffer(table_buf, processed_pandas)
