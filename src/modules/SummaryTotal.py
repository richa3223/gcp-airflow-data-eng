# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

"""
Summary Total data model class
"""

__all__ = ["SummaryTotal"]

from typing import NamedTuple
from modules.Names import Names
from modules.Parsers import Parsers

class SummaryTotal(NamedTuple):
    """Model describing summary total records"""
    report_type: str
    category: str
    pkrd_quantity_sum: int
    pkrd_value_tp_sum: float
    nfsi_quantity_sum: int
    nfsi_value_sum: float
    quantity_variance_sum: int
    value_variance_sum: float
    git_quantity_sum: int
    git_value_sum: float
    pct_of_sales: float
    ptd_ex_git: float
    pct_of_sales_ex_git: float

    def bigquery_dict(self, metadata_fields: dict) -> dict:
        """Returns instance as dict keyed on BigQuery table column names"""
        d = self._asdict()
        metadata_fields.update(d)
        return metadata_fields

    @classmethod
    def from_result(cls, result, report_type: str = Names.TYPE_SUMMARY):
        return cls(
            report_type=report_type,
            category=Parsers.get_attribute(result, Names.DEPOT_CATEGORY, Names.TYPE_SUMMARY),
            pkrd_quantity_sum=result.sum_pkrd_quantity,
            pkrd_value_tp_sum=result.sum_pkrd_value_tp,
            nfsi_quantity_sum=result.sum_nfsi_quantity,
            nfsi_value_sum=result.sum_nfsi_value,
            quantity_variance_sum=result.sum_quantity_variance,
            value_variance_sum=result.sum_value_variance_tp,
            git_quantity_sum=result.sum_git_quantity,
            git_value_sum=result.sum_git_value,
            pct_of_sales=cls.percent_sales(result),
            ptd_ex_git=cls.profits_to_date_ex_git(result),
            pct_of_sales_ex_git=cls.percent_sales_ex_git(result)
        )

    @classmethod
    def profits_to_date_ex_git(cls, result) -> float:
        return result.sum_value_variance_tp - result.sum_git_value

    @classmethod
    def percent_sales(cls, result) -> float:
        return (result.sum_value_variance_tp / result.sum_pkrd_value_tp) * 100

    @classmethod
    def percent_sales_ex_git(cls, result) -> float:
        ptd_ex_git = cls.profits_to_date_ex_git(result)
        return (ptd_ex_git / result.sum_pkrd_value_tp) * 100
    
# fmt: on