# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

"""
Fin Rec data model class
"""

__all__ = ["FinRecData"]

from typing import NamedTuple, Optional
from datetime import date
from modules.FinRecParsers import FinRecParsers
from modules.Names import Names
from modules.Parsers import Parsers
import hashlib

# Schema for merged PKRD and NFSI dataset
class FinRecData(NamedTuple):
    record_date: date
    source_data_type: str
    sku : str
    moveorder_short: Optional[str]
    lot_number: Optional[str]
    depot_id: str
    depot_name: Optional[str]
    depot_category: Optional[str]
    sku_moveorder: Optional[str]
    order_id: Optional[str]
    sku_and_order: Optional[str]
    pkrd_unit_price: float
    pkrd_case_price: float
    pkrd_quantity: Optional[int]
    pkrd_value: Optional[float]
    pkrd_value_tp: Optional[float]
    nfsi_quantity: Optional[int]
    nfsi_value: Optional[float]
    quantity_variance: float
    value_variance: float
    value_variance_tp: float
    fingerprint: str
                    
    def bigquery_dict(self, metadata_fields: dict) -> dict:
        """Returns instance as dict keyed on BigQuery table column names"""
        d = self._asdict()
        metadata_fields.update(d)
        return metadata_fields

    @classmethod
    def from_pkrd(cls, data: dict):
        return cls.from_dataset(Names.TYPE_PKRD, data)
    
    @classmethod
    def from_fresh(cls, data: dict):
        return cls.from_dataset(Names.TYPE_FRESH, data)
    
    @classmethod
    def from_frozen(cls, data: dict):
        return cls.from_dataset(Names.TYPE_FROZEN, data)
    
    @classmethod
    def from_non_nfsi(cls, data: dict):
        return cls.from_dataset(Names.TYPE_NON_NFSI, data)    

    @classmethod
    def from_dataset(cls, type_const: str, data: dict):   
        record_date = FinRecParsers.record_date(type_const, data)
        type = type_const
        item_id = FinRecParsers.item_number(type_const, data)
        mo = FinRecParsers.short_moveorder(type_const, data)
        lot_no = FinRecParsers.lot_number(type_const, data)
        depot_id = FinRecParsers.depot_id(type_const, data)
        depot_name = data[Names.DEPOT_NAME]
        depot_category = data[Names.DEPOT_CATEGORY]
        sku_mo = Parsers.composite_key(item_id, mo)
        order_no = FinRecParsers.order_number(type_const, data)
        sku_and_order = Parsers.composite_key(item_id, order_no)
        unit_price = FinRecParsers.unit_price(type_const, data)
        case_price = FinRecParsers.case_price(type_const, data)
        pkrd_qty = FinRecParsers.pkrd_qty(type_const, data)
        pkrd_val = FinRecParsers.pkrd_val(type_const, data)
        pkrd_val_tp = FinRecParsers.pkrd_val_tp(type_const, data) 
        nfsi_qty = FinRecParsers.nfsi_qty(type_const, data)
        nfsi_val = FinRecParsers.nfsi_val(type_const, data)  
        qty_var = pkrd_qty + nfsi_qty
        value_var = pkrd_val + nfsi_val
        value_var_tp = pkrd_val_tp + nfsi_val      
        hash_input = (
            f'{record_date}'
            f'{type}'
            f'{sku_mo}'
            f'{sku_and_order}'
            f'{depot_id}'
            f'{lot_no}'
            f'{pkrd_qty}'
            f'{nfsi_qty}'
        )
        fingerprint_str = hashlib.sha256(hash_input.encode('utf-8')).hexdigest()
        return cls(
            record_date=record_date,
            source_data_type=type,
            sku=item_id,
            moveorder_short=mo,
            lot_number=lot_no,
            depot_id=depot_id,
            depot_name=depot_name,
            depot_category=depot_category,
            sku_moveorder=sku_mo,
            order_id=order_no,
            sku_and_order=sku_and_order,
            pkrd_unit_price=unit_price,
            pkrd_case_price=case_price,
            pkrd_quantity=pkrd_qty,
            pkrd_value=pkrd_val,
            pkrd_value_tp=pkrd_val_tp,
            nfsi_quantity=nfsi_qty,
            nfsi_value=nfsi_val,
            quantity_variance=qty_var,
            value_variance=round(value_var,4),
            value_variance_tp=round(value_var_tp,4),
            fingerprint=fingerprint_str
        )
    
# fmt: on