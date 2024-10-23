# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

import pytest
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from typing import NamedTuple
from modules.Names import Names
from modules.Parsers import Parsers


class TestParsers:
    """Unit tests for the Parsers class"""

    def test_str_to_bool(self):
        assert Parsers.str_to_bool("TRUE") == True
        assert Parsers.str_to_bool("FALSE") == False
        assert Parsers.str_to_bool("true") == True
        assert Parsers.str_to_bool("false") == False
        assert Parsers.str_to_bool(None) is None
        assert Parsers.str_to_bool("1") is None
        assert Parsers.str_to_bool("0") is None
        assert Parsers.str_to_bool("") is None
        assert Parsers.str_to_bool("INVALID") is None

    def test_str_to_date(self):
        assert Parsers.str_to_date("01/01/2023") == date(2023, 1, 1)
        assert Parsers.str_to_date("31/12/2099") == date(2099, 12, 31)
        assert Parsers.str_to_date("29/02/2024") == date(2024, 2, 29)
        assert Parsers.str_to_date("") is None
        assert Parsers.str_to_date(None) is None
        assert Parsers.str_to_date("30-06-2024") is None
        assert Parsers.str_to_date("29/02/2023") is None

    def test_define_date_range(self):
        default_start_date = datetime.now().date() - relativedelta(months=1)
        default_end_date = datetime.now().date()

        assert Parsers.define_date_range("01/01/2023", "31/12/2023") == {
            Names.START_DATE: date(2023, 1, 1),
            Names.END_DATE: date(2023, 12, 31)
        }
        assert Parsers.define_date_range("31/12/2023", "01/01/2023") == {
            Names.START_DATE: date(2022, 12, 1),
            Names.END_DATE: date(2023, 1, 1)
        }
        assert Parsers.define_date_range("", "") == {
            Names.START_DATE: default_start_date,
            Names.END_DATE: default_end_date
        }
        assert Parsers.define_date_range(None, None) == {
            Names.START_DATE: default_start_date,
            Names.END_DATE: default_end_date
        }
        assert Parsers.define_date_range("31/12/2023", "30/12/2023") == {
            Names.START_DATE: date(2023, 11, 30),
            Names.END_DATE: date(2023, 12, 30)
        }


    def test_default_date_range(self):
        default_start_date = datetime.now().date() - relativedelta(months=1)
        default_end_date = datetime.now().date()

        assert Parsers.default_date_range() == {
            Names.START_DATE: default_start_date,
            Names.END_DATE: default_end_date
        }
        assert Parsers.default_date_range(end_date=date(2023, 10, 31)) == {
            Names.START_DATE: date(2023, 9, 30),
            Names.END_DATE: date(2023, 10, 31)
        }

    def test_effective_date(self):
        assert Parsers.effective_date("01/01/2023") == date(2023, 1, 1)
        assert Parsers.effective_date("") == date.today()
        assert Parsers.effective_date(None) == date.today()

    def test_filter_dates(self):
        assert Parsers.filter_dates("01/01/2023", "31/12/2023") == {
            Names.START_DATE: date(2023, 1, 1),
            Names.END_DATE: date(2023, 12, 31)
        }

    def test_clean_numeric_str(self):
        assert Parsers.clean_numeric_str("1,234,567.89") == "1234567.89"
        assert Parsers.clean_numeric_str("£(56,789.12)") == "-56789.12"
        assert Parsers.clean_numeric_str("-123.456") == "-123.456"
        assert Parsers.clean_numeric_str("Answer=42 !") == "42"
        assert Parsers.clean_numeric_str("#N/A") == ''        

    def test_clean_float_value(self):
        assert Parsers.clean_float_value("£123.45") == 123.45
        assert Parsers.clean_float_value("(123.45)") == -123.45
        assert Parsers.clean_float_value("-123.456") == -123.456
        assert Parsers.clean_float_value("123,456.78912") == 123456.78912
        assert Parsers.clean_float_value("invalid") == 0.0

    def test_clean_int_value(self):
        assert Parsers.clean_int_value("123,456") == 123456
        assert Parsers.clean_int_value("123.45") == 123
        assert Parsers.clean_int_value("987,654.22") == 987654
        assert Parsers.clean_int_value("1,234,567.89") == 1234567
        assert Parsers.clean_int_value("invalid") == 0

    def test_clean_desc(self):
        assert Parsers.clean_desc('"This is a description."') == "This is a description."
        assert Parsers.clean_desc("This is a description.\n") == "This is a description."

    def test_composite_key(self):
        assert Parsers.composite_key("prefix", "suffix") == "prefix_suffix"
        assert Parsers.composite_key("prefix", "") == "prefix_MISSING"
        assert Parsers.composite_key("", "suffix") == "MISSING_suffix"
        assert Parsers.composite_key("", "") == "MISSING_MISSING"

    def test_get_attribute(self):
        class TestObject(NamedTuple):
            attr1: str
            attr2: int

        obj = TestObject("value1", 123)
        assert Parsers.get_attribute(obj, "attr1") == "value1"
        assert Parsers.get_attribute(obj, "attr2") == 123
        assert Parsers.get_attribute(obj, "attr3", "default") == "default"

# fmt: on