#  EKKO table DataFrame with purchase order header details


# EKKO renaming mapping
EKKO_RENAME_MAP = {
    'MANDT':'CLIENT',
    'EBELN':'PURCHASE_ORDER_NUMBER',
    'BUKRS':'COMPANY_CODE',
    'BSART':'PURCHASING_DOCUMENT_TYPE',
    'Del. Indicator':'DELETION_INDICATOR',
    'AEDAT':'LAST_CHANGE_DATE',
    'LIFNR':'SUPPLIER_ID',
    'WAERS':'DOCUMENT_CURRENCY',
    'LIFRE':'INVOICING_PARTY'

}



    # 'Doc. Category':'DOCUMENT_CATEGORY',
    # 'Pyt Terms':'PAYMENT_TERMS',
    # 'Currency':'PO_CURRENCY',
    # 'Exchange Rate':'EXCHANGE_RATE',
    # 'Document Date':'PURCHASING_DOCUMENT_DATE',
# 'Suppl. Vendor':'SUPPLYING_VENDOR',
#     'Invoicing Pty':'INVOICING_PARTY',
#     'Down Payment':'DOWN_PAYMENT_INDICATOR',
#     'VAT Reg. No.':'VAT_REGISTRATION_NUMBER',
#     'Purchasing Org.':'PURCHASING_ORG'