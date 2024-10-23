# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

"""
Mappers module
"""

__all__ = ["Mappers"]

from modules.FinRecParsers import FinRecParsers
from modules.DictUtils import DictUtils
from modules.Names import Names

class Mappers(object):
    """Functions used in Apache Beam Map() transforms"""

    @classmethod
    def add_computed_fields(cls, element, type: str):
        """Enrich data with computed fields"""
        d = element
        d.update(FinRecParsers.add_computed_fields(type, element))
        return d
    
    @classmethod
    def add_depot_ref_data_fields(cls, element, depots):
        """Adds depot name and depot category fields using depot ID as join key"""
        d = element
        d.update(FinRecParsers.decode_depot(element, depots))
        return d
    
    @classmethod
    def add_pricing_data_fields(cls, element, prices):
        """Adds transfer pricing fields using item_id as join key"""
        d = element
        d.update(FinRecParsers.add_pricing(Names.TYPE_PKRD, element, prices))
        return d
    
    @classmethod
    def subset_for_join(cls, element, join_key: str, keys: list[str]=None):
        """Create subset of data ready for join transform"""
        if keys is None:
            keys = element.keys()
        return DictUtils(element).slice(keys).as_keyed_tuple(join_key)