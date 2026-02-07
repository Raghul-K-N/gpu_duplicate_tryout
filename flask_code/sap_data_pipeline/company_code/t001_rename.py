# T001 - Company Code Rename Module
# This module handles the renaming and merging of the T001 table (Company Code) with


T001_RENAME_MAP = {
    'MANDT':'CLIENT',
    'BUKRS':'COMPANY_CODE',
    'ADRNR':'ADDRESS_ID' # legal entity address

}



# Old mapping
# T001_RENAME_MAP = {
#     'Client':'CLIENT',
#     'Company Code':'COMPANY_CODE',
#     'Address':'ADDRESS', # legal entity address
#     'Name':'LE_NAME', # legal entity name
#     'Name 1':'LE_NAME_1',
#     'Name 2':'LE_NAME_2',
#     'Name 3':'LE_NAME_3',
#     'Name 4':'LE_NAME_4',
#     'Street':'LE_STREET',
#     'City':'LE_CITY',
#     'Postal Code':'LE_POSTAL_CODE',
#     'Country':'LE_COUNTRY'

# }