# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

"""
Filters module
"""

__all__ = ["Filters"]

from modules.FinRecData import FinRecData
from modules.Names import Names

class Filters(object):
    """Provides filter methods for Apache Beam Transforms"""

    @classmethod
    def filter_by_types(cls, row: FinRecData, types: list[str], depot_type: str) -> bool:
        """Returns true if row matches specified source data type AND depot category"""
        return (row.source_data_type in types and row.depot_category == depot_type)    
    
    @classmethod
    def filter_by_category(cls, row: FinRecData, category: str) -> bool:
        """Returns true if row matches specified depot category"""
        return row.depot_category.startswith(category)
    
    @classmethod
    def filter_for_dates(cls, row: FinRecData, dates: dict) -> bool:
        """Returns true if range is empty or if row date is in the populated range"""
        start = dates.get(Names.START_DATE, None)
        end = dates.get(Names.END_DATE, None)
        if bool(start) and bool(end):
            return (row.record_date >= start and row.record_date <= end)
        elif bool(start):
            return row.record_date >= start
        elif bool(end):
            return row.record_date <= end
        else:
            return True
        
    @classmethod
    def filter_exclude_moveorder_prefix(cls, row: FinRecData, prefix: str) -> bool:
        """Returns true if moveorder does NOT start with specified prefix"""
        return row.moveorder_short.startswith(prefix) == False
    
    @classmethod
    def filter_exclude_depot_id(cls, row: FinRecData, id: str) -> bool:
        """Returns true if depot ID does NOT start with specified ID"""
        return row.depot_id.startswith(id) == False    
        
# fmt: on