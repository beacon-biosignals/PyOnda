import numpy as np
import tempfile
from pathlib import Path
from pyonda.utils.s3_upload import upload_file_to_s3


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


def save_array_to_lpcm_file_in_s3(array, bucket, key):
    """Save a numpy array in a temp dir as a .lpcm and upload it to s3

    Parameters
    ----------
    array : ndarray
        input numpy array to be saved
    bucket : str
        destination bucket name
    key : str
        destination file key

    Returns
    -------
    upload_status: bool
        False if ClientError is catched during upload
    """
    temp_dir = tempfile.TemporaryDirectory() 
    temp_file_path = Path(temp_dir.name) / "array_to_upload.lpcm"
    save_array_to_lpcm_file(array, temp_file_path)
    upload_status = upload_file_to_s3(temp_file_path, bucket, key)
    temp_dir.cleanup()
    return upload_status