# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

import pytest
from pytest import fixture
from datetime import date
from modules.FinRecData import FinRecData
from modules.Names import Names

"""Defines reusable fixtures for unit tests"""

@pytest.fixture
def pkrd_fresh_row():
    return FinRecData(
                record_date=date(2023, 1, 1),
                source_data_type="PKRD",
                sku="60330045",
                moveorder_short="MM012345",
                lot_number="T123456789",
                depot_id="709",
                depot_name="Depot A",
                depot_category="NFSI Fresh",
                sku_moveorder="60330045_MM012345",
                order_id="8811223",
                sku_and_order="60330045_8811223",
                pkrd_unit_price=1.23,
                pkrd_case_price=4.56,
                pkrd_quantity=-123,
                pkrd_value=-560.88,
                pkrd_value_tp=-560.88,
                nfsi_quantity=0,
                nfsi_value=0.0,
                quantity_variance=-123,
                value_variance=-560.88,
                value_variance_tp=-560.88,
                fingerprint="2873495272329660823"
    )

@pytest.fixture
def pkrd_frozen_row():
    return FinRecData(
                record_date=date(2023, 1, 14),
                source_data_type="PKRD",
                sku="60441156",
                moveorder_short="MM045678",
                lot_number="T123456789",
                depot_id="123",
                depot_name="Depot A",
                depot_category="NFSI Frozen",
                sku_moveorder="60441156_MM045678",
                order_id="88223344",
                sku_and_order="60441156_88223344",
                pkrd_unit_price=2.49,
                pkrd_case_price=6.99,
                pkrd_quantity=-567,
                pkrd_value=-3963.33,
                pkrd_value_tp=-3963.33,
                nfsi_quantity=0,
                nfsi_value=0.0,
                quantity_variance=-567,
                value_variance=-3963.33,
                value_variance_tp=-3963.33,
                fingerprint="9876543210"
    )

@pytest.fixture
def fresh_fresh_row():
    return FinRecData(
                record_date=date(2024, 2, 12),
                source_data_type="NFSI Fresh",
                sku="60330045",
                moveorder_short="MM012345",
                lot_number="",
                depot_id="987",
                depot_name="Depot B",
                depot_category="NFSI Fresh",
                sku_moveorder="60330045_MM012345",
                order_id="8811223",
                sku_and_order="60330045_8811223",
                pkrd_unit_price=0,
                pkrd_case_price=0,
                pkrd_quantity=0,
                pkrd_value=0,
                pkrd_value_tp=0,
                nfsi_quantity=123,
                nfsi_value=560.88,
                quantity_variance=123,
                value_variance=560.88,
                value_variance_tp=560.88,
                fingerprint="1234567890"
    )

@pytest.fixture
def frozen_frozen_row():
    return FinRecData(
                record_date=date(2023, 1, 14),
                source_data_type="NFSI Frozen",
                sku="60441156",
                moveorder_short="MM045678",
                lot_number="",
                depot_id="987",
                depot_name="Depot B",
                depot_category="NFSI Frozen",
                sku_moveorder="60441156_MM045678",
                order_id="88223344",
                sku_and_order="60441156_88223344",
                pkrd_unit_price=0,
                pkrd_case_price=0,
                pkrd_quantity=0,
                pkrd_value=0,
                pkrd_value_tp=0,
                nfsi_quantity=567,
                nfsi_value=3963.33,
                quantity_variance=567,
                value_variance=3963.33,
                value_variance_tp=3963.33,
                fingerprint="9876543210"
    )

@pytest.fixture
def non_nfsi_row():
    return FinRecData(
                record_date=date(2024, 6, 5),
                source_data_type="Non-NFSI",
                sku="60112345",
                moveorder_short="MM023456",
                lot_number="",
                depot_id="XYZ",
                depot_name="Depot C",
                depot_category="Non-NFSI",
                sku_moveorder="60112345_MM023456",
                order_id="88223344",
                sku_and_order="60112345_88223344",
                pkrd_unit_price=0,
                pkrd_case_price=0,
                pkrd_quantity=0,
                pkrd_value=0,
                pkrd_value_tp=0,
                nfsi_quantity=567,
                nfsi_value=3963.33,
                quantity_variance=567,
                value_variance=3963.33,
                value_variance_tp=3963.33,
                fingerprint="9876543210"
    )

@pytest.fixture
def pkrd_dict():
    return {
        "Store": "709",
        "Item No.": "60112345",
        "SMS_ORDER_NUMBER": "16600661",
        "Move Order": "MM04326789/005",
        Names.DEPOT_ID: "709"
    }

@pytest.fixture
def fresh_dict():
    return {
        "DEPOT": "100718",
        "ORDER_NO": "80015432",
        "LPC": "654321",
        Names.DEPOT_ID: "718"
    }

@pytest.fixture
def frozen_dict():
    return {
        "DEPOT": "100989",
        "ORDER_NO": "80016789",
        "LPC": "669876",
        Names.DEPOT_ID: "989"
    }

@pytest.fixture
def non_nfsi_dict():
    return {
        "Customer No": "XYZ",
        "Item No": "60112345",
        "Sales Order No": "MM023456",
        Names.DEPOT_ID: "XYZ"
    }

@pytest.fixture
def sales_dict():
    return {
        "PARTNO": "60334567",
        "SORDNO_ITM1": "MM067890",
        "Textbox268": "321",
        "SMS_ORDER_NUMBER": "89901234",
        Names.DEPOT_ID: "321"
    }

@pytest.fixture
def pkrd_data():
    return {
        "Move Date": "01/01/2023",
        "Item No.": "60330045",
        "Move Order": "MM012345/005",
        "Lot Number": "T123456789",
        "Store": "709",
        "SMS_ORDER_NUMBER": "8811223",
        "Qty": "-123",
        "Value": "-560.88",
        Names.DEPOT_ID: "709",
        Names.DEPOT_NAME: "Depot A",
        Names.DEPOT_CATEGORY: "NFSI Fresh",
        Names.SKU_MO: "60330045_MM012345",
        Names.UNIT_PRICE: 1.23,
        Names.CASE_PRICE: 4.56,
    }

@pytest.fixture
def fresh_data():
    return {
        "ACTUAL_TRAN_DATE": "12/02/2024",
        "LPC": "330045",
        "SORDNO_ITM1": "MM012345",
        "DEPOT": "100987",
        "ORDER_NO": "8811223",
        "PACKS_RECEIVED": "123",
        "TOTAL_COST": "560.88",
        Names.DEPOT_ID: "987",
        Names.DEPOT_NAME: "Depot B",
        Names.DEPOT_CATEGORY: "NFSI Fresh",
        Names.SKU_MO: "60330045_MM012345",                  
    }

@pytest.fixture
def frozen_data():
    return {
        "ACTUAL_TRAN_DATE": "14/01/2023",
        "LPC": "441156",
        "SORDNO_ITM1": "MM045678",
        "DEPOT": "100987",
        "ORDER_NO": "88223344",
        "PACKS_RECEIVED": "567",
        "TOTAL_COST": "3963.33",
        Names.DEPOT_ID: "987",
        Names.DEPOT_NAME: "Depot B",
        Names.DEPOT_CATEGORY: "NFSI Frozen",
        Names.SKU_MO: "60441156_MM045678",         
    }

@pytest.fixture
def non_nfsi_data():
    return {
        "Invoice Date": "05/06/2024",
        "Customer No": "XYZ",
        "Item No": "60112345",
        "Sales Order No": "MM023456",
        "PO # (1)": "88223344",
        "QTY In Cases": "567",
        "Total Price": "3963.33",
        Names.DEPOT_ID: "XYZ",
        Names.DEPOT_NAME: "Depot C",
        Names.DEPOT_CATEGORY: "Non-NFSI",   
        Names.SKU_MO: "60112345_MM023456"
    }
# fmt: on