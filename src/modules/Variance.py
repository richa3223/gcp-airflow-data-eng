# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

"""
Fin Rec quantity and value variance data model class
"""

__all__ = ["Variance"]

from typing import NamedTuple, Optional
from modules.Names import Names
from modules.Parsers import Parsers

class Variance(NamedTuple):
    """Model for describing variance aggregations""" 
    variance_type: str
    source_data_type: Optional[str]
    record_date: Optional[str]
    depot_id: Optional[str]
    depot_name: Optional[str]
    depot_category: str
    total_pkrd_quantity: int
    total_pkrd_value_tp : float
    total_nfsi_quantity: int
    total_nfsi_value: float
    total_quantity_variance: int
    total_value_variance_tp: float
    moveorder_short: Optional[str]
    sku: Optional[str]
    is_git: bool
    git_quantity: Optional[int]
    git_value: Optional[float]

    def bigquery_dict(self, metadata_fields: dict) -> dict:
        """Returns instance as dict keyed on BigQuery table column names"""
        d = self._asdict()
        metadata_fields.update(d)
        return metadata_fields

    @classmethod
    def from_result(cls, result, var_type: str):
        return cls(
            variance_type=var_type,
            source_data_type=Parsers.get_attribute(result, Names.SOURCE_DATA_TYPE),
            record_date=Parsers.get_attribute(result, Names.RECORD_DATE),
            depot_id=Parsers.get_attribute(result, Names.DEPOT_ID),
            depot_name=Parsers.get_attribute(result, Names.DEPOT_NAME),
            depot_category=result.depot_category,
            total_pkrd_quantity=result.total_pkrd_quantity,
            total_pkrd_value_tp=result.total_pkrd_value_tp,
            total_nfsi_quantity=result.total_nfsi_quantity,
            total_nfsi_value=result.total_nfsi_value,
            total_quantity_variance=result.total_quantity_variance,
            total_value_variance_tp=result.total_value_variance_tp,
            moveorder_short=Parsers.get_attribute(result, Names.MO_SHORT),
            sku=Parsers.get_attribute(result, Names.SKU),
            is_git=cls.is_goods_in_transit(result),
            git_quantity=cls.get_git_quantity(result),
            git_value=cls.get_git_value(result)
        )
    
    @classmethod
    def is_goods_in_transit(cls, result) -> bool:
        """Determines if instance qualifies as goods in transit"""
        pkrd_zero = (result.total_pkrd_quantity == 0 and result.total_pkrd_value_tp == 0)    
        nfsi_zero = (result.total_nfsi_quantity == 0 and result.total_nfsi_value == 0)
        if (pkrd_zero and nfsi_zero == False) or (pkrd_zero == False and nfsi_zero):
            return True
        else:
            return False
        
    @classmethod
    def get_git_quantity(cls, result) -> int:
        return result.total_quantity_variance if cls.is_goods_in_transit(result) else 0
    
    @classmethod
    def get_git_value(cls, result) -> float:
        return result.total_value_variance_tp if cls.is_goods_in_transit(result) else 0    

# fmt: on