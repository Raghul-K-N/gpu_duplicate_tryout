# LFBK: Vendor Bank Details Column Rename Mapping


# Client	Supplier	Bank Country	Bank Key	Bank Account	Part. Bank Type	Account holder	Appl.obj.change

LFBK_RENAME_COLUMNS = {
    'MANDT':'CLIENT',
    'LIFNR':'SUPPLIER_ID',
    'BANKS':'BANK_COUNTRY',
    'BANKL':'BANK_KEY',
    'BANKN':'BANK_ACCOUNT',
    'BKONT':'BANK_CONTROL_KEY',
    'BVTYP':'PARTNER_BANK_TYPE',
    'KOINH':'ACCOUNT_HOLDER',
    'EBPP_ACCNAME':"ACCOUNT_NAME",
    'ChangeIndicator':'APPLICATION_CHANGE_INDICATOR'
}

