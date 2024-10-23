# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

"""
CSV File handling utility class
"""

__all__ = ["CsvFileUtils, EXTRA_COLS_KEY, MISSING_COLS_VALUE"]

from modules.FileUtils import FileUtils
from csv import reader, DictReader

class CsvFileUtils(object):
    """CSV File handling utility class"""

    EXTRA_COLS_KEY='extras'
    MISSING_COLS_VALUE='MISSING_COLUMN_INPUT'
    TS_FORMAT='%Y%m%d'

    @classmethod
    def csv_column_names(cls, f) -> list[str]:
        """Returns CSV column names from file"""
        if FileUtils.is_gcs_path(f):
            return CsvFileUtils.csv_column_names_from_gcs(f)
        else:
            return CsvFileUtils.csv_column_names_from_local_file(f)
    
    @classmethod
    def csv_column_names_from_gcs(cls, f):
        """Returns CSV column names from file on Google Cloud Storage (GCS)"""
        return FileUtils.get_gcs_csv_column_names(f)

    @classmethod
    def csv_column_names_from_local_file(cls, f):
        """Returns CSV column names from local file"""   
        try:
            with open(f, encoding='utf-8-sig',mode='r') as csv_file:
                header_row = csv_file.readline().strip()
            return next(reader([header_row]))
        except OSError as ex:
            print('Error loading file [%s] : %s', f, ex)

    @classmethod
    def csv_row_as_dict(cls, row, csv_cols) -> dict:
        """Returns a single CSV row as a dict, tagging any malformed rows"""
        try:
            dict_reader = DictReader(
                                [row], 
                                fieldnames=csv_cols, 
                                restkey=cls.EXTRA_COLS_KEY,
                                restval=cls.MISSING_COLS_VALUE
                            )
            row = dict_reader.__next__()
            return row
        except Exception as ex:
            print('Error parsing CSV row "%s" --> [%s]', row, ex)

    @classmethod
    def is_row_well_formed(cls, row, csv_cols, schema_cols) -> bool:
        """Validates row column names, column count and row length against schema"""
        schema_cols_set = set(schema_cols)
        csv_cols_set = set(csv_cols)
        schema_mismatch_cols = schema_cols_set.difference(csv_cols_set)
        csv_mismatch_cols = csv_cols_set.difference(schema_cols_set)
        has_extra_values = cls.EXTRA_COLS_KEY in row.keys()
        has_schema_column_mismatch = len(schema_mismatch_cols) > 0
        has_missing_column_values = cls.MISSING_COLS_VALUE in row.values()

        if has_schema_column_mismatch:
            print(f'Row has schema columns missing or incorrect --> Schema cols : {schema_mismatch_cols} vs CSV cols : {csv_mismatch_cols}')
        elif has_extra_values:
            print(f'Row has extra column values --> {row[cls.EXTRA_COLS_KEY]}')
        elif has_missing_column_values:
            print(f'Row has missing column values --> {cls.missing_value_col_names(row)}')

        return not(has_schema_column_mismatch or has_extra_values or has_missing_column_values)

    @classmethod
    def missing_value_col_names(cls, row: dict) -> list[str]:
        """Returns column names where row has a missing value placeholder"""
        return [k for (k,v) in row.items() if v == cls.MISSING_COLS_VALUE]
    
    @classmethod
    def ts_suffix(cls) -> str:
        """Returns a timestamp.csv filename suffix string"""
        return f'{FileUtils.ts_str(cls.TS_FORMAT)}.csv'
    
# fmt: on