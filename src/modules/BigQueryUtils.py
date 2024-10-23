# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

"""
BigQuery utility class
"""

__all__ = ["BigQueryUtils"]

from datetime import date, datetime, timezone
from modules.Names import Names
import uuid
import copy

class BigQueryUtils(object):
    """BigQuery utility class"""

    TIMESTAMP_COL_NAME = 'submission_datetime'
    DEFAULT_DICT_NAME_KEY = 'name'
    DEFAULT_DICT_VALUE_KEY = 'value' 

    @classmethod
    def schema_columns(cls, schema: list[dict]) -> list[str]:
        """Ordered list of BigQuery table column names from AVRO table schema"""
        return [col['name'] for col in schema]
    
    @classmethod
    def columns_as_dict(cls, record: list[dict], 
                        name_key=DEFAULT_DICT_NAME_KEY, 
                        value_key=DEFAULT_DICT_VALUE_KEY) -> dict:
        """Returns schema columns as dictionary keyed by columnn name"""
        col_dict = {}
        for col in record:
            key = col[name_key]
            value = col[value_key]
            col_dict[key] = value
        return col_dict
    
    @classmethod
    def schema_compliant_row_as_dict(cls, record: dict, schema: list[dict]) -> dict:
        """Returns row as dict keyed on column name per schema definition and ordering"""
        bq_row_dict = {}
        for col_name in cls.schema_columns(schema):
            value = cls.utc_ts() if col_name == cls.TIMESTAMP_COL_NAME else record[col_name]
            bq_row_dict[col_name] = value
        return bq_row_dict

    @classmethod
    def utc_ts(cls) -> datetime:
        """UTC datetime"""
        return datetime.now(timezone.utc)
    
    @classmethod
    def correlation_id(cls) -> str:
        """Generates a UUID4 string value"""
        return uuid.uuid4().hex
    
    @classmethod
    def metadata_fields(cls) -> dict:
        """Generates a common set of metadata fields for each table"""
        return {
            Names.UTC_TS: cls.utc_ts(),
            Names.CORRELATION_ID: cls.correlation_id(),
            Names.RECORD_STATUS: Names.RECORD_STATUS_ACTIVE
        }


    @classmethod
    def metadata_plus_date(cls, src_dict: dict, eff_date: date) -> dict:
        """Extends metadata fields with effective date attribute"""
        d = copy.deepcopy(src_dict)
        d.update({Names.EFF_DATE: eff_date})
        return d
    
    @classmethod
    def metadata_plus_valid_from(cls, src_dict: dict) -> dict:
        """Extends metadata fields with valid_from attribute"""
        d = copy.deepcopy(src_dict)
        d.update({Names.VALID_FROM: d[Names.UTC_TS].replace(second=0,microsecond=0)})
        return d

# fmt: on