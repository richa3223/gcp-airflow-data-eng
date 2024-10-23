# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned
from __future__ import annotations
"""
Dictionary dataset utilities
"""

__all__ = ["DictUtils"]

from collections import UserDict

class DictUtils(UserDict):
    """Dictionary dataset adapter utility class"""

    def slice(self, keys: list[str]) -> DictUtils:
        """Returns dictionary slice for provided keys"""
        return DictUtils({ k: self.data[k] for k in keys })
    
    def as_keyed_tuple(self, key: str):
        """Returns dictionary as a tuple keyed by specified value"""
        return self.data[key], self.data

# fmt: on