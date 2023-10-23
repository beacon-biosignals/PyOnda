from pyonda.save_arrow import (
    save_table_to_arrow_file, 
    save_table_to_s3
)
from pyonda.load_arrow import load_table_from_arrow_file, load_table_from_arrow_file_in_s3

from tests.fixtures import (
    aws_credentials,
    signal_arrow_table_path,
    lpcm_file_path,
    lpcm_zst_file_path,
    s3,
    signal_arrow_table_s3_url,
    assert_signal_arrow_dataframes_equal
)


def test_save_table_to_arrow_file(signal_arrow_table_path, tmpdir):
    ref_table = load_table_from_arrow_file(signal_arrow_table_path, processed_pandas=False)
    save_table_to_arrow_file(tmpdir / "test_table.arrow", ref_table, ref_table.schema)
    saved_table = load_table_from_arrow_file(tmpdir / "test_table.arrow", processed_pandas=False)
    assert saved_table == ref_table

    
def test_save_table_to_s3(signal_arrow_table_path, s3):
    ref_table = load_table_from_arrow_file(signal_arrow_table_path, processed_pandas=False)
    save_table_to_s3(ref_table, ref_table.schema, "mock-bucket", "test_table.arrow")
    saved_table = load_table_from_arrow_file_in_s3("s3://mock-bucket/test_table.arrow", processed_pandas=False)
    assert saved_table == ref_table