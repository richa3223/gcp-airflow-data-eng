# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

"""
Fin Rec data parsing class
"""

__all__ = ["FinRecParsers"]

from datetime import date
from modules.Names import Names
from modules.Parsers import Parsers

class FinRecParsers(object):
    """Fin Rec data parsing class"""

    @classmethod
    def add_computed_fields(cls, type: str, data: dict) -> dict:
        depot = cls.depot_id(type, data)
        sku = cls.item_number(type, data)
        order = cls.order_number(type, data)
        mo = cls.short_moveorder(type, data)
        sku_mo = Parsers.composite_key(sku, mo)
        sku_order_id = Parsers.composite_key(sku, order)

        return {
            Names.DEPOT_ID: depot,
            Names.MO_SHORT: mo,
            Names.ORDER_ID: order,
            Names.SKU: sku,
            Names.SKU_MO: sku_mo,
            Names.SKU_ORDER: sku_order_id,
        }
            
    @classmethod
    def decode_depot(cls, data: dict, depots: dict) -> dict:
        decode = {}
        depot_id = data[Names.DEPOT_ID]
        if depot_id in depots:
            depot_info = depots[depot_id]
            decode[Names.DEPOT_NAME] = depot_info[Names.DEPOT_NAME]
            decode[Names.DEPOT_CATEGORY] = depot_info[Names.DEPOT_CATEGORY]
        return decode
    
    @classmethod
    def add_pricing(cls, type: str, data: dict, prices: dict) -> dict:
        pricing = {}
        cols = Names.COLS[type]
        item_id = data.get(cols[Names.SKU_KEY],'')
        if item_id in prices:
            price_info = prices[item_id]
            pricing[Names.UNIT_PRICE] = price_info[Names.TP_UNIT_PRICE]
            pricing[Names.CASE_PRICE] = price_info[Names.TP_CASE_PRICE]
        return pricing
    
    @classmethod
    def record_date(cls, type: str, data: dict) -> date:
        cols = Names.COLS[type]
        rd_str = data.get(cols[Names.DATE_KEY], None)
        rd_datetime = Parsers.str_to_date(rd_str, Parsers.MAX_DATE)
        return rd_datetime

    @classmethod
    def short_moveorder(cls, type: str, data: dict) -> str:
        cols = Names.COLS[type]
        mo = data.get(cols[Names.MO_KEY], Names.MISSING_MO)
        if len(mo) > 0 and (type == Names.TYPE_PKRD or type == Names.TYPE_SALES):
            return mo.split('/')[0]
        else:
            return mo
            
    @classmethod
    def item_number(cls, type: str, data: dict) -> str:
        cols = Names.COLS[type]
        item_id = data.get(cols[Names.SKU_KEY], '')
        if len(item_id) > 0 and (type == Names.TYPE_FRESH or type == Names.TYPE_FROZEN):
            return str(int(item_id)+60000000)
        elif (len(item_id) > 1 and len(item_id) < 8) and type == Names.TYPE_NON_NFSI:
            return str(int(item_id)+60000000)
        else:
            return item_id
        
    @classmethod
    def lot_number(cls, type: str, data: dict) -> str:
        cols = Names.COLS[type]
        if type == Names.TYPE_PKRD:
            return data.get(cols[Names.LOT_KEY], '')
        else:
            return ''
            
    @classmethod
    def depot_id(cls, type: str, data: dict) -> str:
        cols = Names.COLS[type]
        depot = data.get(cols[Names.DEPOT_KEY],'')
        if len(depot) > 0 and (type == Names.TYPE_FRESH or type == Names.TYPE_FROZEN):
            return depot[-3:]
        else:
            return depot
        
    @classmethod
    def order_number(cls, type: str, data: dict) -> str:
        cols = Names.COLS[type]
        return data.get(cols[Names.ORDER_KEY], None)
    
    @classmethod
    def unit_price(cls, type: str, data: dict) -> float:
        if type == Names.TYPE_PKRD:
            return float(data.get(Names.UNIT_PRICE,0))
        else: 
            return 0
        
    @classmethod
    def case_price(cls, type: str, data: dict) -> float:
        if type == Names.TYPE_PKRD:
            return float(data.get(Names.CASE_PRICE,0))
        else: 
            return 0

    @classmethod
    def pkrd_qty(cls, type: str, data: dict):
        cols = Names.COLS[type]
        if type == Names.TYPE_PKRD:
            qty = data.get(cols[Names.PKRD_QTY_KEY],'0')
            return Parsers.clean_int_value(qty)
        else:
            return data.get(Names.PKRD_QTY, 0)
        
    @classmethod
    def pkrd_val(cls, type: str, data: dict):
        cols = Names.COLS[type]
        if type == Names.TYPE_PKRD:
            pval = data.get(cols[Names.PKRD_VAL_KEY],'0')
            return Parsers.clean_float_value(pval)
        else:
            return data.get(Names.PKRD_VAL, 0)
        
    @classmethod
    def pkrd_val_tp(cls, type: str, data: dict):
        if type == Names.TYPE_PKRD:
            qty = cls.pkrd_qty(type, data)
            price = cls.case_price(type, data)
            return round((qty * price), 5)
        else:
            return data.get(Names.PKRD_VAL_TP, 0)

    @classmethod
    def nfsi_qty(cls, type: str, data: dict):
        cols = Names.COLS[type]
        if type == Names.TYPE_PKRD:
            return 0
        else:
            val = data.get(cols[Names.NFSI_QTY_KEY],'0')
            return Parsers.clean_int_value(val)
    
    @classmethod
    def nfsi_val(cls, type: str, data: dict):
        cols = Names.COLS[type]
        if type == Names.TYPE_PKRD:
            return 0
        else:
            val = data.get(cols[Names.NFSI_VAL_KEY],'0')
            return Parsers.clean_float_value(val)

# fmt: on