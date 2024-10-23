# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

import pytest
from pytest import FixtureRequest
from typing import Dict
from modules.FinRecData import FinRecData
from modules.Names import Names


class TestFinRecData:
    """Unit tests for the FinRecData class"""

    @pytest.mark.parametrize(
        "type, data, expected",
        [
            (
                Names.TYPE_PKRD,
                'pkrd_data',
                'pkrd_fresh_row',
            ),
            (
                Names.TYPE_FRESH,
                'fresh_data',
                'fresh_fresh_row',
            ),
            (
                Names.TYPE_NON_NFSI,
                'non_nfsi_data',
                'non_nfsi_row',
            ),
            (
                Names.TYPE_FROZEN,
                'frozen_data',
                'frozen_frozen_row',
            ),
        ],
    )
    def test_from_dataset(self, type: str,data: Dict, expected: FinRecData, request: FixtureRequest):
        data = request.getfixturevalue(data)
        expected = request.getfixturevalue(expected)
        frd = FinRecData.from_dataset(type, data)
        assert frd.record_date == expected.record_date
        assert frd.source_data_type == expected.source_data_type
        assert frd.sku == expected.sku
        assert frd.moveorder_short == expected.moveorder_short
        assert frd.lot_number == expected.lot_number
        assert frd.depot_id == expected.depot_id
        assert frd.depot_name == expected.depot_name
        assert frd.depot_category == expected.depot_category
        assert frd.sku_moveorder == expected.sku_moveorder
        assert frd.order_id == expected.order_id
        assert frd.sku_and_order == expected.sku_and_order
        assert frd.pkrd_unit_price == expected.pkrd_unit_price
        assert frd.pkrd_case_price == expected.pkrd_case_price
        assert frd.pkrd_quantity == expected.pkrd_quantity
        assert frd.pkrd_value == expected.pkrd_value
        assert frd.pkrd_value_tp == expected.pkrd_value_tp
        assert frd.nfsi_quantity == expected.nfsi_quantity
        assert frd.nfsi_value == expected.nfsi_value
        assert frd.quantity_variance == expected.quantity_variance
        assert frd.value_variance == expected.value_variance
        assert frd.value_variance_tp == expected.value_variance_tp

# fmt: on