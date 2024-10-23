# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

"""
Parsing utility class
"""

__all__ = ["Parsers"]

from typing import NamedTuple, Any
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from modules.Names import Names

class Parsers(object):
    """Provides a number of reusable data value parsing functions"""

    DATE_FORMAT = "%d/%m/%Y"
    MAX_DATE = datetime.strptime('31/12/2099',DATE_FORMAT).date()
    BOOL_MAP = {"TRUE": True, "FALSE": False}

    @classmethod
    def str_to_bool(cls, str_val) -> bool | None:
        """Decodes TRUE|True|FALSE|False string values to bool"""
        val_str = str_val.upper() if isinstance(str_val, str) else None
        return cls.BOOL_MAP.get(val_str, None)
    
    @classmethod
    def str_to_date(cls, date_str: str, default = None) -> date | None:
        """Converts string with format dd/mm/yyyy to date"""
        if bool(date_str):
            try:
                return datetime.strptime(date_str, "%d/%m/%Y").date()
            except ValueError as err:
                print(f'Incorrect date format : {err}')
        return default
    
    @classmethod
    def define_date_range(cls, start_str: str, end_str: str) -> dict:
        """Generate and validate date range from input variables"""
        start = cls.str_to_date(start_str)
        end = cls.str_to_date(end_str)
        if not (bool(start) and bool(end)):
            return cls.default_date_range()
        elif (bool(start) and bool(end)) and start >= end:
            print('Start date must be less than end date')
            return cls.default_date_range(end_date=end)
        else:
            print(f'Date range defined : start {start}, end {end}')
            return {
                Names.START_DATE: start,
                Names.END_DATE: end                
            }
        
    @classmethod
    def default_date_range(cls, end_date: date = None, delta: int = 1) -> dict:
        """Generates a date range starting one month prior to current date"""
        end = datetime.now().date() if end_date is None else end_date
        start = end - relativedelta(months=delta)
        print(f'Applying default date range : start {start}, end {end}')
        return {
            Names.START_DATE: start,
            Names.END_DATE: end
        }
    
    @classmethod
    def effective_date(cls, eff_str: str) -> date:
        """Returns effective date or if null, current date"""
        eff_date = cls.str_to_date(eff_str)
        return datetime.now().date() if eff_date is None else eff_date
    
    @classmethod
    def filter_dates(cls, start_str: str, end_str: str) -> dict:
        """Wraps reporting dates as a dict"""
        return cls.define_date_range(start_str, end_str)
    
    @classmethod
    def clean_numeric_str(cls, str_val: str) -> str | None:
        """Strips out any undesirable non-numeric characters from a string"""
        junk = ',)£*_?/\!#@%^&+={}<>~`abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ '
        return str_val.translate(str.maketrans('(', '-', junk))
        
    @classmethod
    def clean_float_value(cls, str_val: str) -> float:
        """Scrubs and transforms string currency values to 5DP float"""
        clean_str = cls.clean_numeric_str(str_val)
        try:
            return round(float(clean_str), 5)
        except ValueError as err:
            print(f'Parsing info : {err}. Defaulting value to 0')
            return 0
    
    @classmethod
    def clean_int_value(cls, str_val: str) -> int:
        """Scrubs and transforms string values to int"""
        clean_str = cls.clean_numeric_str(str_val)
        int_str = clean_str.split('.')[0]
        try:
            return int(int_str)
        except ValueError as err:
            print(f'Parsing info : {err}. Defaulting value to 0')
            return 0
    
    @classmethod
    def clean_desc(cls, str_val: str) -> str:
        """Scrubs descriptions to remove quotation marks and newlines"""
        clean_str = str_val.translate(str.maketrans({'"': None}))
        return clean_str.rstrip()
    
    @classmethod
    def composite_key(cls, prefix: str, suffix: str) -> str:
        p = prefix if bool(prefix) else Names.MISSING
        s = suffix if bool(suffix) else Names.MISSING
        return "{}_{}".format(p, s)
    
    @classmethod
    def get_attribute(cls, obj: NamedTuple, attr_name: str, default: Any|None = None):
        """Returns default value if attribute not present on object"""
        return getattr(obj, attr_name, default)    