# BKPF - Header level data rename
BKPF_RENAME_MAP = {
    'MANDT':'CLIENT',
    'Pstg per.var.':'REGION',
    'BUKRS':'COMPANY_CODE',
    'BELNR':'DOCUMENT_NUMBER',
    'GJAHR':'FISCAL_YEAR',
    'AWTYP':'REF_TRANSACTION',
    'AWKEY':'REFERENCE_KEY',
    'BLART':'DOCUMENT_TYPE',
    'XBLNR':'INVOICE_NUMBER',
    'HWAER':'LOCAL_CURRENCY',
    'WAERS':'DOCUMENT_CURRENCY',

    'CPUDT':'ENTERED_DATE',
    'BUDAT':'POSTED_DATE',
    'BLDAT':'INVOICE_DATE',
    'REINDAT':'INVOICE_RECEIPT_DATE',
    'KURSF':'EXCHANGE_RATE',
    'LIFNR':'SUPPLIER_ID',
    'BKTXT':'DOCUMENT_HEADER_TEXT',

    # newly added
    'MONAT':'FISCAL_PERIOD',
    'USNAM':'ENTERED_BY',
    'PPNAM':'PARKED_BY',
    'SNAME':'POSTED_BY',   # Need to confirm
    'BSTAT':'DOCUMENT_STATUS',
    'UPDDT':'LAST_CHANGED_DATE',
    'KZWRS':'GROUP_CURRENCY',
    'KZKRS':'EXCHANGE_RATE_TYPE',


}

    # 'Part. Bank Type':'PARTNER_BANK_TYPE',
    # 'Pmnt Block':'PAYMENT_BLOCK',
    # 'Pmt Method':'PAYMENT_METHOD',
    # 'Pyt Terms':'PAYMENT_TERMS',
    # 'Baseline Date':'BASELINE_DATE',
    # 'Planning date':'DUE_DATE',
    #     'Payer':'PAYER',
    # 'Reason code':'REASON_CODE',
    # 'Debit/Credit':'DEBIT_CREDIT_INDICATOR_HEADER_LEVEL',
    # 'Clrng doc.':'CLEARING_DOCUMENT_NUMBER',
    # 'Year':'YEAR',
    # 'Reversed with':'REVERSE_DOCUMENT_NUMBER',
    # 'Transaction Code':'TRANSACTION_CODE',
    # 'Clearing':'PAYMENT_DATE',
    # 'Amount in LC':'TOTAL_AMOUNT_LC',
    # 'Amount':'TOTAL_AMOUNT',
    # 'Exchange rate 2':'EXCHANGE_RATE_USD',
    # 'User name':'ENTERED_BY',
    # 'Text':'HEADER_TEXT',