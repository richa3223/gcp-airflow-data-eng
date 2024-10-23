# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

"""
Transfer Pricing data model class
"""

__all__ = ["Pricing"]

from typing import NamedTuple, Optional
from datetime import datetime, date
from modules.Names import Names
from modules.Parsers import Parsers

# Schema for Transfer Pricing record
class Pricing(NamedTuple):
    pricing_date: date
    sku: str
    min: Optional[str]
    pin: Optional[str]
    long_desc: str
    room: str
    room_two: Optional[str]
    trading_category: Optional[str]
    pack_weight: float
    case_size: int
    case_weight: float
    rm: float
    pack: float
    lab: float
    dist: float
    oh: float
    depot_loss: float
    total: float
    rm_case: float
    pack_case: float
    lab_case: float
    dist_case: float
    oh_case: float
    depot_loss_case: float
    total_case: float    

    def bigquery_dict(self, metadata_fields: dict) -> dict:
        """Returns instance as dict keyed on BigQuery table column names"""
        d = self._asdict()
        metadata_fields.update(d)
        return metadata_fields
    
    @classmethod
    def from_dataset(cls, data: dict):
        """Returns an instance of Pricing from source dataset"""
        cols = Names.COLS[Names.TYPE_PRICING]
        pricing_date = data.get(cols.get(Names.DATE_KEY, None), datetime.now().date())
        sku = data[cols[Names.SKU_KEY]]
        min = data.get(cols[Names.MIN_KEY], None)
        pin = data.get(cols[Names.PIN_KEY], None)
        long_desc = data[cols[Names.DESC_KEY]]
        room = data[cols[Names.ROOM_KEY]]
        room_2 = data.get(cols[Names.ROOM_2_KEY], None)
        trading_category = data.get(cols[Names.TRAD_CATEGORY_KEY], None)
        pack_weight = data.get(cols[Names.PACK_WEIGHT_KEY], 0)
        case_size = data.get(cols[Names.CASE_SIZE_KEY], 0)
        case_weight = data.get(cols[Names.CASE_WEIGHT_KEY], 0)
        rm = data.get(cols[Names.RM_KEY], 0)
        pack = data.get(cols[Names.PACK_KEY], 0)
        lab = data.get(cols[Names.LAB_KEY], 0)
        dist = data.get(cols[Names.DIST_KEY], 0)
        oh = data.get(cols[Names.OH_KEY], 0)
        depot_loss = data.get(cols[Names.DEPOT_LOSS_KEY], 0)
        total = data.get(cols[Names.PRICE_TOTAL_KEY], 0)
        rm_case = data.get(cols[Names.RM_CASE_KEY], 0)
        pack_case = data.get(cols[Names.PACK_CASE_KEY], 0)
        lab_case = data.get(cols[Names.LAB_CASE_KEY], 0)
        dist_case = data.get(cols[Names.DIST_CASE_KEY], 0)
        oh_case = data.get(cols[Names.OH_CASE_KEY], 0)
        depot_loss_case = data.get(cols[Names.DEPOT_LOSS_CASE_KEY], 0)
        total_case = data.get(cols[Names.PRICE_TOTAL_CASE_KEY], 0)
        return cls(
            pricing_date=pricing_date,
            sku=sku,
            min=min,
            pin=pin,
            long_desc=Parsers.clean_desc(long_desc),
            room=Parsers.clean_desc(room),
            room_two=Parsers.clean_desc(room_2),
            trading_category=Parsers.clean_desc(trading_category),
            pack_weight=Parsers.clean_float_value(pack_weight),
            case_size=Parsers.clean_int_value(case_size),
            case_weight=Parsers.clean_float_value(case_weight),
            rm=Parsers.clean_float_value(rm),
            pack=Parsers.clean_float_value(pack),
            lab=Parsers.clean_float_value(lab),
            dist=Parsers.clean_float_value(dist),
            oh=Parsers.clean_float_value(oh),
            depot_loss=Parsers.clean_float_value(depot_loss),
            total=Parsers.clean_float_value(total),
            rm_case=Parsers.clean_float_value(rm_case),
            pack_case=Parsers.clean_float_value(pack_case),
            lab_case=Parsers.clean_float_value(lab_case),
            dist_case=Parsers.clean_float_value(dist_case),
            oh_case=Parsers.clean_float_value(oh_case),
            depot_loss_case=Parsers.clean_float_value(depot_loss_case),
            total_case=Parsers.clean_float_value(total_case)
        )

# fmt: on