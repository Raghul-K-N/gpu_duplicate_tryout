# BSEG  - Line item level data

#Column -  Item  -  not mapped yet
BSEG_RENAME_MAP = {
    'MANDT':'CLIENT',
    'BUKRS':'COMPANY_CODE',
    'BELNR':'DOCUMENT_NUMBER',
    'GJAHR':'FISCAL_YEAR',
    'BUZEI':'LINE_ITEM_NUMBER',
    'Debit/Credit':'DEBIT_CREDIT_INDICATOR',
    'G/L':'GL_ACCOUNT_NUMBER',
    'DMBTR':'LINEITEM_AMOUNT_IN_LOCAL_CURRENCY',
    'WRBTR':'LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY',
    'Purchasing Doc.':'PURCHASE_ORDER_NUMBER',
    'Item.1':'PO_ITEM_NUMBER',
    'Tax code':'TAX_CODE',
    'SGTXT':'ITEM_TEXT',

    # newly added
    'AUGBL':'CLEARING_DOCUMENT_NUMBER',
    'BVTYP':'PARTNER_BANK_TYPE',
    'ZLSCH':'PAYMENT_METHOD',
    'ZFBDT':'BASELINE_DATE',
    'EMPFB':'PAYEE',
    'RSTGR':'REASON_CODE',
    'ZTERM':'PAYMENT_TERMS',
    'ZBD1P':'DISCOUNT_PERCENTAGE_1',
    'ZBD2P':'DISCOUNT_PERCENTAGE_2',
    'FDTAG':'DUE_DATE',  # planning Date  (Confirm is assumption is correct)
    'ZLSPR':'PAYMENT_BLOCK',
    'WERKS':'PLANT',
    'AUGDT':'PAYMENT_DATE', # Clearing Date
    'MWSKZ':'TAX_INDICATOR',
    'LIFNR':'SUPPLIER_ID',
    'H_WAERS':'GROUP_CURRENCY',

}




# Used Before
#     'Pstg per.var.':'REGION',
    # 'Criterion':'ITEM_TEXT',   # sometimes appears as 'Criterion' instead of 'Short Text'
    # 'Line item ID':'LINE_ITEM_ID',
