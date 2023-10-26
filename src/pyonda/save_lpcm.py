import numpy as np
import tempfile
from pathlib import Path
from pyonda.utils.s3_upload import upload_file_to_s3
from pyonda.utils.compression import compress_file_to_zst
from botocore.client import BaseClient


def save_array_to_lpcm_file(array, output_path):
    """Save numpy array to .lpcm binary file

    Parameters
    ----------
    array : ndarray
        input numpy array to be saved
    output_path : str or Path
        output file path
    """
    output_path = str(output_path)
    if output_path[-5:]!=".lpcm":
        raise ValueError(f"output path should have .lpcm extension (you have {output_path[:-5]})")

    holder = np.memmap(
        output_path, 
        dtype=array.dtype, 
        mode='w+', 
        shape=array.shape, 
        order='C'
    )
    holder[:] = array


def save_array_to_lpcm_zst_file(array, output_path):
    """Save numpy array to .lpcm.zst compressed binary file

    Parameters
    ----------
    array : ndarray
        input numpy array to be saved
    output_path : str or Path
        output file path
    """
    if str(output_path)[-9:]!=".lpcm.zst":
        raise ValueError(f"output path should have .lpcm.zst extension (you have {str(output_path)[:-9]})")
    
    output_path = Path(output_path)
    temp_dir = tempfile.TemporaryDirectory()

    lpcm_path = Path(temp_dir.name, output_path.stem)
    try:
        save_array_to_lpcm_file(array, lpcm_path)
        compress_file_to_zst(lpcm_path, output_path.parent)
    finally:
        temp_dir.cleanup()


def save_array_to_lpcm_file_in_s3(array, bucket, key, client:BaseClient=None):
    """Save a numpy array in a temp dir as a .lpcm and upload it to s3

    Parameters
    ----------
    array : ndarray
        input numpy array to be saved
    bucket : str
        destination bucket name
    key : str
        destination file key
    client: BaseClient, default=None
        boto3 client instance

    Returns
    -------
    upload_status: bool
        False if ClientError is catched during upload
    """
    temp_dir = tempfile.TemporaryDirectory() 
    temp_file_path = Path(temp_dir.name) / "array_to_upload.lpcm"
    try:
        save_array_to_lpcm_file(array, temp_file_path)
        upload_file_to_s3(temp_file_path, bucket, key, client)
    finally:
        temp_dir.cleanup()


def save_array_to_lpcm_zst_file_in_s3(array, bucket, key, client:BaseClient=None):
    """Save a numpy array in a temp dir as a .lpcm and upload it to s3

    Parameters
    ----------
    array : ndarray
        input numpy array to be saved
    bucket : str
        destination bucket name
    key : str
        destination file key
    compress : bool, default=False
        if true, compress file to .zst 
    client: BaseClient, default=None
        boto3 client instance

    Returns
    -------
    upload_status: bool
        False if ClientError is catched during upload
    """
    temp_dir = tempfile.TemporaryDirectory() 
    temp_file_path = Path(temp_dir.name) / "array_to_upload.lpcm"
    try:
        save_array_to_lpcm_file(array, temp_file_path)
        compress_file_to_zst(temp_file_path, Path(temp_dir.name))

        compressed_file_path = Path(temp_dir.name) / 'array_to_upload.lpcm.zst'
        upload_file_to_s3(compressed_file_path, bucket, key, client)
    finally:
        temp_dir.cleanup()