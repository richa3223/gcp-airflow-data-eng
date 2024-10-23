# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

import pytest
from pytest import FixtureRequest
from datetime import date
from typing import Dict
from modules.FinRecParsers import FinRecParsers
from modules.Names import Names

class TestFinRecParsers:
    """Unit tests for the FinRecParsers class"""

    @pytest.mark.parametrize(
        "type, data, expected",
        [
            (
                Names.TYPE_PKRD,
                'pkrd_dict',
                {
                    Names.DEPOT_ID: "709",
                    Names.MO_SHORT: "MM04326789",
                    Names.ORDER_ID: "16600661",
                    Names.SKU: "60112345",
                    Names.SKU_MO: "60112345_MM04326789",
                    Names.SKU_ORDER: "60112345_16600661",
                },
            ),
            (
                Names.TYPE_FRESH,
                'fresh_dict',
                {
                    Names.DEPOT_ID: "718",
                    Names.MO_SHORT: "MISSING_MO",
                    Names.ORDER_ID: "80015432",
                    Names.SKU: "60654321",
                    Names.SKU_MO: "60654321_MISSING_MO",
                    Names.SKU_ORDER: "60654321_80015432",
                },
            ),
            (
                Names.TYPE_FROZEN,
                'frozen_dict',
                {
                    Names.DEPOT_ID: "989",
                    Names.MO_SHORT: "MISSING_MO",
                    Names.ORDER_ID: "80016789",
                    Names.SKU: "60669876",
                    Names.SKU_MO: "60669876_MISSING_MO",
                    Names.SKU_ORDER: "60669876_80016789",
                },
            ),
            (
                Names.TYPE_NON_NFSI,
                'non_nfsi_dict',
                {
                    Names.DEPOT_ID: "XYZ",
                    Names.MO_SHORT: "MM023456",
                    Names.ORDER_ID: None,
                    Names.SKU: "60112345",
                    Names.SKU_MO: "60112345_MM023456",
                    Names.SKU_ORDER: "60112345_MISSING",
                },
            ),            
            (
                Names.TYPE_SALES,
                'sales_dict',
                {
                    Names.DEPOT_ID: "321",
                    Names.MO_SHORT: "MM067890",
                    Names.ORDER_ID: "89901234",
                    Names.SKU: "60334567",
                    Names.SKU_MO: "60334567_MM067890",
                    Names.SKU_ORDER: "60334567_89901234",
                },
            ),
        ],
    )
    def test_add_computed_fields(self, type: str, data: Dict, expected: Dict, request: FixtureRequest):
        data = request.getfixturevalue(data)
        assert FinRecParsers.add_computed_fields(type, data) == expected

    @pytest.mark.parametrize(
        "data, depots, expected",
        [
            (
                {Names.DEPOT_ID: "123"},
                {
                    "123": {
                        Names.DEPOT_NAME: "Depot A",
                        Names.DEPOT_CATEGORY: "Fresh",
                    }
                },
                {Names.DEPOT_NAME: "Depot A", Names.DEPOT_CATEGORY: "Fresh"},
            ),
            (
                {Names.DEPOT_ID: "456"},
                {
                    "123": {
                        Names.DEPOT_NAME: "Depot A",
                        Names.DEPOT_CATEGORY: "Fresh",
                    }
                },
                {},
            ),
        ],
    )
    def test_decode_depot(self, data: Dict, depots: Dict, expected: Dict):
        assert FinRecParsers.decode_depot(data, depots) == expected

    @pytest.mark.parametrize(
        "type, data, prices, expected",
        [
            (
                Names.TYPE_PKRD,
                {
                    "Item No.": "60112345"
                },
                {
                    "60112345": {
                        Names.TP_UNIT_PRICE: 1.23,
                        Names.TP_CASE_PRICE: 4.56,
                    }
                },
                {
                    Names.UNIT_PRICE: 1.23,
                    Names.CASE_PRICE: 4.56
                },
            ),
            (
                Names.TYPE_FRESH,
                {
                    Names.SKU: "654321"
                },
                {
                    "60654321": {
                        Names.TP_UNIT_PRICE: 1.23,
                        Names.TP_CASE_PRICE: 4.56,
                    }
                },
                {},
            ),
        ],
    )
    def test_add_pricing(self, type: str, data: Dict, prices: Dict, expected: Dict):
        assert FinRecParsers.add_pricing(type, data, prices) == expected

    @pytest.mark.parametrize(
        "type, data, expected",
        [
            (
                Names.TYPE_PKRD,
                {
                    "Move Date": "29/02/2024"
                },
                date(2024, 2, 29),
            ),
            (
                Names.TYPE_FRESH,
                {
                    "ACTUAL_TRAN_DATE": "01-01-2023"
                },
                date(2099, 12, 31),
            ),
            (
                Names.TYPE_FROZEN,
                {
                    "ACTUAL_TRAN_DATE": "01/01/2023"
                },
                date(2023, 1, 1),
            ),
            (
                Names.TYPE_NON_NFSI,
                {
                    "Invoice Date": "01/01/2023"
                },
                date(2023, 1, 1),
            ),            
            (
                Names.TYPE_SALES,
                {
                    "CUSTREQDTE_SOR": "2023/01/01"
                },
                date(2099, 12, 31),
            ),
        ],
    )
    def test_record_date(self, type: str, data: Dict, expected: date):
        assert FinRecParsers.record_date(type, data) == expected

    @pytest.mark.parametrize(
        "type, data, expected",
        [
            (
                Names.TYPE_PKRD,
                {
                    "Move Order": "MM050123/003"
                },
                "MM050123",
            ),
            (
                Names.TYPE_SALES,
                {
                    "SORDNO_ITM1": "MM058765"
                },
                "MM058765",
            ),
        ],
    )
    def test_short_moveorder(self, type: str, data: Dict, expected: str):
        assert FinRecParsers.short_moveorder(type, data) == expected

    @pytest.mark.parametrize(
        "type, data, expected",
        [
            (
                Names.TYPE_FRESH,
                {
                    "LPC": "330045"
                },
                "60330045",
            ),
            (
                Names.TYPE_FROZEN,
                {
                    "LPC": "445566"
                },
                "60445566",
            ),
            (
                Names.TYPE_NON_NFSI,
                {
                    "Item No": "0998877"
                },
                "60998877",
            ), 
            (
                Names.TYPE_NON_NFSI,
                {
                    "Item No": "60897654"
                },
                "60897654",
            ),                        
            (
                Names.TYPE_SALES,
                {
                    "PARTNO": "60556677"
                },
                "60556677",
            ),
        ],
    )
    def test_item_number(self, type: str, data: Dict, expected: str):
        assert FinRecParsers.item_number(type, data) == expected

    @pytest.mark.parametrize(
        "type, data, expected",
        [
            (
                Names.TYPE_PKRD,
                {
                    "Lot Number": "T123456789"
                },
                "T123456789",
            ),
            (
                Names.TYPE_FRESH,
                {
                    "Lot Number": "T123456789"
                },
                "",
            ),
            (
                Names.TYPE_FROZEN,
                {
                    "Lot Number": "T123456789"
                },
                "",
            ),
            (
                Names.TYPE_SALES,
                {
                    "Lot Number": "T123456789"
                },
                "",
            ),
        ],
    )
    def test_lot_number(self, type: str, data: Dict, expected: str):
        assert FinRecParsers.lot_number(type, data) == expected

    @pytest.mark.parametrize(
        "type, data, expected",
        [
            (
                Names.TYPE_PKRD,
                {
                    "Store": "222"
                },
                "222",
            ),
            (
                Names.TYPE_FRESH,
                {
                    "DEPOT": "100456"
                },
                "456",
            ),
            (
                Names.TYPE_FROZEN,
                {
                    "DEPOT": "100456"
                },
                "456",
            ),
            (
                Names.TYPE_NON_NFSI,
                {
                    "Customer No": "456"
                },
                "456",
            ),            
            (
                Names.TYPE_SALES,
                {
                    "Textbox268": "799"
                },
                "799",
            ),
        ],
    )
    def test_depot_id(self, type: str, data: Dict, expected: str):
        assert FinRecParsers.depot_id(type, data) == expected

    @pytest.mark.parametrize(
        "type, data, expected",
        [
            (
                Names.TYPE_PKRD,
                {
                    "SMS_ORDER_NUMBER": "8811223"
                },
                "8811223",
            ),
            (
                Names.TYPE_FRESH,
                {
                    "ORDER_NO": "8822334"
                },
                "8822334",
            ),
            (
                Names.TYPE_FROZEN,
                {
                    "ORDER_NO": "8833445"
                },
                "8833445",
            ),
            (
                Names.TYPE_NON_NFSI,
                {
                    "PO # (1)": "8866778"
                },
                "8866778",
            ),            
            (
                Names.TYPE_SALES,
                {
                    "SMS_ORDER_NUMBER": "8844556"
                },
                "8844556",
            ),
        ],
    )
    def test_order_number(self, type: str, data: Dict, expected: str):
        assert FinRecParsers.order_number(type, data) == expected

    @pytest.mark.parametrize(
        "type, data, expected",
        [
            (
                Names.TYPE_PKRD,
                {
                    Names.UNIT_PRICE: "1.23"
                },
                1.23,
            ),
            (
                Names.TYPE_FRESH,
                {
                    Names.UNIT_PRICE: "1.23"
                },
                0,
            ),
            (
                Names.TYPE_FROZEN,
                {
                    Names.UNIT_PRICE: "1.23"
                },
                0,
            ),
            (
                Names.TYPE_SALES,
                {
                    Names.UNIT_PRICE: "1.23"
                },
                0,
            ),
        ],
    )
    def test_unit_price(self, type: str, data: Dict, expected: float):
        assert FinRecParsers.unit_price(type, data) == expected

    @pytest.mark.parametrize(
        "type, data, expected",
        [
            (
                Names.TYPE_PKRD,
                {
                    Names.CASE_PRICE: "4.56"
                },
                4.56,
            ),
            (
                Names.TYPE_FRESH,
                {
                    Names.CASE_PRICE: "4.56"
                },
                0,
            ),
            (
                Names.TYPE_FROZEN,
                {
                    Names.CASE_PRICE: "4.56"
                },
                0,
            ),
            (
                Names.TYPE_SALES,
                {
                    Names.CASE_PRICE: "4.56"
                },
                0,
            ),
        ],
    )
    def test_case_price(self, type: str, data: Dict, expected: float):
        assert FinRecParsers.case_price(type, data) == expected

    @pytest.mark.parametrize(
        "type, data, expected",
        [
            (
                Names.TYPE_PKRD,
                {
                    "Qty": "(123.99)"
                },
                -123,
            ),
            (
                Names.TYPE_FRESH,
                {
                    Names.NFSI_QTY_KEY: "(345.67)"
                },
                0,
            ),
            (
                Names.TYPE_FROZEN,
                {
                    Names.NFSI_QTY_KEY: "21,789.01"
                },
                0,
            ),
            (
                Names.TYPE_SALES,
                {
                    Names.NFSI_QTY_KEY: "-445.67"
                },
                0,
            ),
        ],
    )
    def test_pkrd_qty(self, type: str, data: Dict, expected: int):
        assert FinRecParsers.pkrd_qty(type, data) == expected

    @pytest.mark.parametrize(
        "type, data, expected",
        [
            (
                Names.TYPE_PKRD,
                {
                    "Value": "(123.45)"
                },
                -123.45,
            ),
            (
                Names.TYPE_FRESH,
                {
                    Names.NFSI_VAL_KEY: "345.67"
                },
                0.0,
            ),
            (
                Names.TYPE_FROZEN,
                {
                    Names.NFSI_VAL_KEY: "£21,789.01"
                },
                0.0,
            ),
            (
                Names.TYPE_SALES,
                {Names.NFSI_VAL_KEY: "-445.67"},
                0.0,
            ),
        ],
    )

    def test_pkrd_val(self, type: str, data: Dict, expected: float):
        assert FinRecParsers.pkrd_val(type, data) == expected

    @pytest.mark.parametrize(
        "type, data, expected",
        [
            (
                Names.TYPE_PKRD,
                {
                    "Qty": "123",
                    Names.CASE_PRICE: "4.56"
                },
                560.88,
            ),
            (
                Names.TYPE_FRESH,
                {
                    Names.NFSI_QTY_KEY: "123",
                    Names.CASE_PRICE: "4.56"
                },
                0.0,
            ),
            (
                Names.TYPE_FROZEN,
                {
                    Names.NFSI_QTY_KEY: "123",
                    Names.CASE_PRICE: "4.56"
                },
                0.0,
            ),
            (
                Names.TYPE_SALES,
                {
                    Names.NFSI_QTY_KEY: "123",
                    Names.CASE_PRICE: "4.56"
                },
                0.0,
            ),
        ],
    )

    def test_pkrd_val_tp(self, type: str, data: Dict, expected: float):
        assert FinRecParsers.pkrd_val_tp(type, data) == expected

    @pytest.mark.parametrize(
        "type, data, expected",
        [
            (
                Names.TYPE_PKRD,
                {
                    Names.PKRD_QTY_KEY: "123"
                },
                0,
            ),
            (
                Names.TYPE_FRESH,
                {
                    "PACKS_RECEIVED": "456"
                },
                456,
            ),
            (
                Names.TYPE_FROZEN,
                {
                    "PACKS_RECEIVED": "789"
                },
                789,
            ),
            (
                Names.TYPE_NON_NFSI,
                {
                    "QTY In Cases": "789"
                },
                789,
            ),            
            (
                Names.TYPE_SALES,
                {
                    Names.NFSI_QTY_KEY: "123"
                },
                0,
            ),
        ],
    )

    def test_nfsi_qty(self, type: str, data: Dict, expected: int):
        assert FinRecParsers.nfsi_qty(type, data) == expected

    @pytest.mark.parametrize(
        "type, data, expected",
        [
            (
                Names.TYPE_PKRD,
                {
                    Names.PKRD_VAL_KEY: "123.45"
                },
                0.0,
            ),
            (
                Names.TYPE_FRESH,
                {
                    "TOTAL_COST": "123.45"
                },
                123.45,
            ),
            (
                Names.TYPE_FROZEN,
                {
                    "TOTAL_COST": "123.45"
                },
                123.45,
            ),
            (
                Names.TYPE_NON_NFSI,
                {
                    "Total Price": "123.45"
                },
                123.45,
            ),            
            (
                Names.TYPE_SALES,
                {
                    Names.NFSI_VAL_KEY: "123.45"
                },
                0.0,
            ),
        ],
    )
    def test_nfsi_val(self, type: str, data: Dict, expected: float):
        assert FinRecParsers.nfsi_val(type, data) == expected

# fmt: on
