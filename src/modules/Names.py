# fmt: off
# pylint: disable=abstract-method,unnecessary-dunder-call,expression-not-assigned

"""
Defines constants to support data parsing and dataset management
"""

__all__ = ["Names"]

class Names(object):
    """Defines constants for parsing and dataset management"""

    # Data set types
    TYPE_PKRD = 'PKRD'
    TYPE_FRESH = 'NFSI Fresh'
    TYPE_FROZEN = 'NFSI Frozen'
    TYPE_NON_NFSI = 'Non-NFSI'
    TYPE_SALES = 'SALES'
    TYPE_DEPOTS = 'DEPOTS'
    TYPE_PRICING = 'PRICING'
    TYPE_SUMMARY = 'SUMMARY'
    SOURCE_DATA_TYPE = 'source_data_type'

    # Depot look-up column names 
    DEPOT_ID = 'depot_id'
    DEPOT_NAME = 'depot_name'
    DEPOT_CATEGORY = 'depot_category'

    # Pricing look-up column names
    TP_SKU = 'FB Ref'
    TP_UNIT_PRICE = 'Total'
    TP_CASE_PRICE = 'Total_case'

    # Computed column names
    ITEM_ID = 'item_id'
    SKU = 'sku'
    SKU_MO = 'sku_moveorder'
    MO_SHORT = 'moveorder_short'
    UNIT_PRICE = 'unit_price'
    CASE_PRICE = 'case_price'
    ORDER_ID = 'order_id'
    SKU_ORDER = 'sku_and_order_id'
    PKRD_QTY = 'pkrd_qty'
    PKRD_VAL = 'pkrd_value'
    PKRD_VAL_TP = 'pkrd_value_tp'

    # Date field names
    START_DATE = 'start_date'
    END_DATE = 'end_date'
    EFF_DATE = 'effective_date'
    RECORD_DATE = 'record_date'


    # Common metadata fields for BigQuery tables
    UTC_TS = 'created_ts'
    VALID_FROM = 'valid_from'
    CORRELATION_ID = 'correlation_id'
    RECORD_STATUS = 'record_status'

    # Record status values
    RECORD_STATUS_ACTIVE = 'ACTIVE'
    RECORD_STATUS_INACTIVE = 'INACTIVE'
    RECORD_STATUS_VALID = 'VALID'
    RECORD_STATUS_INVALID = 'INVALID'
    RECORD_STATUS_ERROR = 'ERROR'


    # Column name mapping keys
    DATE_KEY = 'DATE'
    SKU_KEY = 'SKU'
    MO_KEY = 'MO'
    LOT_KEY = 'LOT'
    DEPOT_KEY = 'DEPOT'
    ORDER_KEY = 'ORDER_NUM'
    PKRD_VAL_KEY = 'PKRD_VAL'
    NFSI_QTY_KEY = 'NFSI_QTY'
    PKRD_QTY_KEY = 'PKRD_QTY'
    NFSI_VAL_KEY = 'NFSI_VAL'
    MIN_KEY = 'MIN'
    PIN_KEY = 'PIN'
    DESC_KEY = 'DESC'
    ROOM_KEY = 'ROOM'
    ROOM_2_KEY = "ROOM_2"
    TRAD_CATEGORY_KEY = 'TRAD_CATEGORY'
    PACK_WEIGHT_KEY = 'PACK_WEIGHT'
    CASE_SIZE_KEY = 'CASE_SIZE'
    CASE_WEIGHT_KEY = 'CASE_WEIGHT'
    RM_KEY = 'RM'
    PACK_KEY = 'PACK'
    LAB_KEY = 'LAB'
    DIST_KEY = 'DIST'
    OH_KEY = 'OH'
    DEPOT_LOSS_KEY = 'DEPOT_LOSS'
    PRICE_TOTAL_KEY = 'PRICE_TOTAL'
    RM_CASE_KEY = 'RM_CASE'
    PACK_CASE_KEY = 'PACK_CASE'
    LAB_CASE_KEY = 'LAB_CASE'
    DIST_CASE_KEY = 'DIST_CASE'
    OH_CASE_KEY = 'OH_CASE'
    DEPOT_LOSS_CASE_KEY = 'DEPOT_LOSS_CASE'
    PRICE_TOTAL_CASE_KEY = 'PRICE_TOTAL_CASE'    


    COLS = {
        TYPE_PKRD: {
            DATE_KEY: 'Move Date',
            SKU_KEY: 'Item No.',
            MO_KEY: 'Move Order',
            LOT_KEY: 'Lot Number',
            DEPOT_KEY: 'Store',
            ORDER_KEY: 'SMS_ORDER_NUMBER',
            PKRD_QTY_KEY: 'Qty',
            PKRD_VAL_KEY: 'Value'
        },
        TYPE_FRESH: {
            DATE_KEY: 'ACTUAL_TRAN_DATE',  
            SKU_KEY: 'LPC',
            MO_KEY: 'SORDNO_ITM1',
            DEPOT_KEY: 'DEPOT',
            ORDER_KEY: 'ORDER_NO',
            NFSI_QTY_KEY: 'PACKS_RECEIVED',
            NFSI_VAL_KEY: 'TOTAL_COST'                       
        },
        TYPE_FROZEN: {
            DATE_KEY: 'ACTUAL_TRAN_DATE',        
            SKU_KEY: 'LPC',
            MO_KEY: 'SORDNO_ITM1',        
            DEPOT_KEY: 'DEPOT',
            ORDER_KEY: 'ORDER_NO',
            NFSI_QTY_KEY: 'PACKS_RECEIVED',
            NFSI_VAL_KEY: 'TOTAL_COST'                  
        },
        TYPE_NON_NFSI: {
            DATE_KEY: 'Invoice Date',        
            SKU_KEY: 'Item No',
            MO_KEY: 'Sales Order No',
            DEPOT_KEY: 'Customer No',
            ORDER_KEY: 'PO # (1)',
            NFSI_QTY_KEY: 'QTY In Cases',
            NFSI_VAL_KEY: 'Total Price'               
        },
        TYPE_SALES: {
            DATE_KEY: 'CUSTREQDTE_SOR', # SOCLDATE ??
            SKU_KEY: 'PARTNO',
            MO_KEY: 'SORDNO_ITM1',
            DEPOT_KEY: 'Textbox268',
            ORDER_KEY: 'SMS_ORDER_NUMBER',
            PKRD_QTY_KEY: '',
            PKRD_VAL_KEY: '',
            NFSI_QTY_KEY: 'SO_DESPATCHED_QUANTITY', # SORDQTY ??, QTY_IN_KG ??, DESPATCHED_QTY_IN_KG ??
            NFSI_VAL_KEY: ''               
        },
        TYPE_PRICING: {
            DATE_KEY: 'pricing_date',
            SKU_KEY: 'FB Ref',
            MIN_KEY: 'MIN',
            PIN_KEY: 'PIN',
            DESC_KEY: 'Description',
            ROOM_KEY: 'Room',
            ROOM_2_KEY: 'Room 2',
            TRAD_CATEGORY_KEY: 'Trading Category',
            PACK_WEIGHT_KEY: 'Pack Weight',
            CASE_SIZE_KEY: 'Case Size',
            CASE_WEIGHT_KEY: 'Case Weight',
            RM_KEY: 'RM',
            PACK_KEY: 'Pack',
            LAB_KEY: 'Lab',
            DIST_KEY: 'Dist',
            OH_KEY: 'OH',
            DEPOT_LOSS_KEY: 'Depot Loss',
            PRICE_TOTAL_KEY: 'Total',
            RM_CASE_KEY: 'RM_case',
            PACK_CASE_KEY: 'Pack_case',
            LAB_CASE_KEY: 'Lab_case',
            DIST_CASE_KEY: 'Dist_case',
            OH_CASE_KEY: 'OH_case',
            DEPOT_LOSS_CASE_KEY: 'Depot Loss_case',
            PRICE_TOTAL_CASE_KEY: 'Total_case'   
        }
    }

    # Sales Order slice keys
    SALES_SLICE = [
        SKU_MO,
        COLS[TYPE_SALES][ORDER_KEY],
        SKU_ORDER,
        COLS[TYPE_SALES][MO_KEY],
        ORDER_ID
    ]

    # Fresh and Frozen slice keys
    NFSI_SLICE = [
        ORDER_ID,
        SKU
    ]

    # Join related constants
    JOIN_MATCH = 'JOIN_MATCH'
    MISSING = 'MISSING'
    MISSING_MO = 'MISSING_MO'

    # Variance report types
    FRESH_MO_VAR = 'fresh-moveorder'
    FRESH_SKU_VAR = 'fresh-sku'
    FROZEN_DEPOT_DATE_VAR = 'frozen-depot-date'
    FROZEN_DEPOT_SKU_VAR = 'frozen-depot-sku'
    FROZEN_SKU_VAR = 'frozen-sku'
    NON_NFSI_MO_VAR = 'non-nfsi-moveorder'

    # GCP Project ID options key
    GCP_PROJ_KEY = 'project'

    # BigQuery table names
    DATASET_INTERNAL = 'mm_fin_internal'
    TABLE_FIN_REC_DATA = 'fin_rec_data'
    TABLE_FIN_REC_VAR = 'fin_rec_variance'
    TABLE_FIN_REC_PRICING = 'fin_rec_pricing'
    TABLE_FIN_REC_SUMMARY = 'fin_rec_summary'


# fmt: on