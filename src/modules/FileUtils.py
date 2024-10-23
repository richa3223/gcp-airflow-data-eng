# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

"""
File handling utility class
"""

__all__ = ["FileUtils"]

from pathlib import Path
from datetime import datetime
from google.cloud import storage

class FileUtils(object):
    """File handling utility class"""

    GCS_PREFIX = 'gs:'

    @classmethod
    def is_gcs_path(cls, f) -> bool:
        """True if file path is Google Cloud Storage (GCS)"""
        file_path = Path(f)
        return file_path.parts[0] == cls.GCS_PREFIX
    
    @classmethod
    def get_gcs_csv_column_names(cls, f):
        """Gets first line of GCS file e.g. CSV header row"""
        file_bytes = FileUtils.get_gcs_file_as_bytes(f)
        first_row_bytes = file_bytes.split(b'\n')[0]
        first_row_str = first_row_bytes.decode('utf-8-sig')
        return first_row_str.rstrip().split(',')
    
    @classmethod
    def get_gcs_file_as_text(cls, f):
        """GCS blob for file as text"""
        blob = FileUtils.get_gcs_file_blob(f)
        return blob.download_as_text()

    @classmethod
    def get_gcs_file_as_bytes(cls, f):
        """GCS blob for file as bytes"""
        blob = FileUtils.get_gcs_file_blob(f)
        return blob.download_as_bytes()

    @classmethod
    def get_gcs_file_blob(cls, f):
        """Returns GCS file as Blob"""
        bucket_name = FileUtils.get_gcs_file_bucket_name(f)
        file_path = FileUtils.get_gcs_file_path(f)
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        return bucket.blob(file_path)

    @classmethod
    def get_gcs_file_bucket_name(cls, f) -> str:
        """Extracts GCS bucket name from file path"""
        gcs_path_parts = Path(f).parts
        return gcs_path_parts[1]

    @classmethod
    def get_gcs_file_path(cls, f) -> str:
        """Extracts file path portion of GCS URI"""
        gcs_path_parts = Path(f).parts
        file_path_parts = list(gcs_path_parts[2:len(gcs_path_parts)])
        file_path = "/".join(file_path_parts)
        return file_path
    
    @classmethod
    def ts_str(cls, format: str = '%Y%m%d%H%M%S') -> str:
        """Generates a UNIX epoch timestamp string"""
        return datetime.now().strftime(format)

# fmt: on