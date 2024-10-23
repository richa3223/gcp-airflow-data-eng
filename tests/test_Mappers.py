# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

import pytest
from pytest import fixture, FixtureRequest
from typing import Dict
from modules.Mappers import Mappers
from modules.Names import Names

class TestMappers:
    """Unit tests for the Mappers class"""

    @pytest.mark.parametrize(
        "element, type, expected",
        [
            (
                "pkrd_dict",
                Names.TYPE_PKRD,
                {
                    "Store": "709",
                    "Item No.": "60112345",
                    "SMS_ORDER_NUMBER": "16600661",
                    "Move Order": "MM04326789/005",
                    Names.DEPOT_ID: "709",
                    Names.MO_SHORT: "MM04326789",
                    Names.ORDER_ID: "16600661",
                    Names.SKU: "60112345",
                    Names.SKU_MO: "60112345_MM04326789",
                    Names.SKU_ORDER: "60112345_16600661",
                },
            ),
            (
                "fresh_dict",
                Names.TYPE_FRESH,
                {
                    "DEPOT": "100718",
                    "ORDER_NO": "80015432",
                    "LPC": "654321",
                    Names.DEPOT_ID: "718",
                    Names.MO_SHORT: "MISSING_MO",
                    Names.ORDER_ID: "80015432",
                    Names.SKU: "60654321",
                    Names.SKU_MO: "60654321_MISSING_MO",
                    Names.SKU_ORDER: "60654321_80015432",
                },
            ),
            (
                "frozen_dict",
                Names.TYPE_FROZEN,
                {
                    "DEPOT": "100989",
                    "ORDER_NO": "80016789",
                    "LPC": "669876",
                    Names.DEPOT_ID: "989",
                    Names.MO_SHORT: "MISSING_MO",
                    Names.ORDER_ID: "80016789",
                    Names.SKU: "60669876",
                    Names.SKU_MO: "60669876_MISSING_MO",
                    Names.SKU_ORDER: "60669876_80016789",
                },
            ),
            (
                "non_nfsi_dict",
                Names.TYPE_NON_NFSI,
                {
                    "Customer No": "XYZ",
                    "Item No": "60112345",
                    "Sales Order No": "MM023456",
                    Names.DEPOT_ID: "XYZ",
                    Names.MO_SHORT: "MM023456",
                    Names.ORDER_ID: None,
                    Names.SKU: "60112345",
                    Names.SKU_MO: "60112345_MM023456",
                    Names.SKU_ORDER: "60112345_MISSING",
                },
            ),            
            (
                "sales_dict",
                Names.TYPE_SALES,
                {
                    "PARTNO": "60334567",
                    "SORDNO_ITM1": "MM067890",
                    "Textbox268": "321",
                    "SMS_ORDER_NUMBER": "89901234",
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
    def test_add_computed_fields(self, element: Dict, type: str, expected: Dict, request: FixtureRequest):
        element = request.getfixturevalue(element)
        assert Mappers.add_computed_fields(element, type) == expected

    @pytest.mark.parametrize(
        "element, depots, expected",
        [
            (
                {Names.DEPOT_ID: "123"},
                {
                    "123": {
                        Names.DEPOT_NAME: "Depot A",
                        Names.DEPOT_CATEGORY: "Fresh",
                    }
                },
                {
                    Names.DEPOT_ID: "123",
                    Names.DEPOT_NAME: "Depot A",
                    Names.DEPOT_CATEGORY: "Fresh"
                },
            ),
            (
                {Names.DEPOT_ID: "456"},
                {
                    "123": {
                        Names.DEPOT_NAME: "Depot A",
                        Names.DEPOT_CATEGORY: "Fresh",
                    }
                },
                {Names.DEPOT_ID: "456"},
            ),
        ],
    )
    def test_add_depot_ref_data_fields(self, element: Dict, depots: Dict, expected: Dict):
        assert Mappers.add_depot_ref_data_fields(element, depots) == expected

    @pytest.mark.parametrize(
        "element, prices, expected",
        [
            (
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
                    "Item No.": "60112345",
                    Names.UNIT_PRICE: 1.23,
                    Names.CASE_PRICE: 4.56
                },
            ),
            (
                {
                    Names.SKU: "654321"
                },
                {
                    "60654321": {
                        Names.TP_UNIT_PRICE: 3.45,
                        Names.TP_CASE_PRICE: 6.78,
                    }
                },
                {Names.SKU: "654321"},
            ),
        ],
    )
    def test_add_pricing_data_fields(self, element: Dict, prices: Dict, expected: Dict):
        assert Mappers.add_pricing_data_fields(element, prices) == expected

    @pytest.mark.parametrize(
        "element, join_key, keys, expected",
        [
            (
                "pkrd_dict",
                Names.DEPOT_ID,
                None,
                ("709", {
                    "Store": "709",
                    "Item No.": "60112345",
                    "SMS_ORDER_NUMBER": "16600661",
                    "Move Order": "MM04326789/005",
                    Names.DEPOT_ID: "709"
                }),
            ),
            (
                "fresh_dict",
                Names.DEPOT_ID,
                None,
                ("718", {
                    "DEPOT": "100718",
                    "ORDER_NO": "80015432",
                    "LPC": "654321",
                    Names.DEPOT_ID: "718"
                }),
            ),
            (
                "frozen_dict",
                Names.DEPOT_ID,
                None,
                ("989", {
                    "DEPOT": "100989",
                    "ORDER_NO": "80016789",
                    "LPC": "669876",
                    Names.DEPOT_ID: "989"
                }),
            ),
            (
                "non_nfsi_dict",
                Names.DEPOT_ID,
                None,
                ("XYZ", {
                    "Customer No": "XYZ",
                    "Item No": "60112345",
                    "Sales Order No": "MM023456",
                    Names.DEPOT_ID: "XYZ"
                }),
            ),            
            (
                "sales_dict",
                Names.DEPOT_ID,
                None,
                ("321", {
                    "PARTNO": "60334567",
                    "SORDNO_ITM1": "MM067890",
                    "Textbox268": "321",
                    "SMS_ORDER_NUMBER": "89901234",
                    Names.DEPOT_ID: "321"
                }),
            ),
            (
                "pkrd_dict",
                Names.DEPOT_ID,
                ["Store", "Item No.", Names.DEPOT_ID],
                ("709", {
                    "Store": "709",
                    "Item No.": "60112345",
                    Names.DEPOT_ID: "709"
                }),
            ),
        ],
    )
    def test_subset_for_join(self, element: Dict, join_key: str, keys: list[str], expected: tuple, request: FixtureRequest):
        element = request.getfixturevalue(element)
        if keys is None:
            assert Mappers.subset_for_join(element, join_key, None) == expected
        else:
            assert Mappers.subset_for_join(element, join_key, keys) == expected

# fmt: on