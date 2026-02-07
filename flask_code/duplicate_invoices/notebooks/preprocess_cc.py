import re


def preprocess(df):
    df['PrimaryKey'] = df['AP_PLANT'].astype(str) + '-' + df['VOUCHER'].astype(str) + '-' + df['CHECK_NUMBER'].astype(str) \
    + '-' + df['SUPPLIER'].astype(str) + '-' + df['INVOICE_NUMBER'].astype(str) \
    + '-' + df['INVOICE_AMOUNT'].astype(str) + '-' + df['INVOICE_DATE'].astype(str)

    df['PrimaryKeySimple'] = df.groupby("PrimaryKey").ngroup()
    df['PrimaryKeySimple'] = df['PrimaryKeySimple'].astype(str)

    df['INVOICE_NUMBER'] = df['INVOICE_NUMBER'].apply(lambda x: x.strip().upper())
    df['SUPPLIER_NAME'] = df['SUPPLIER_NAME'].apply(lambda x: x.strip())

    invoice_pattern = re.compile(r'(~1|~2|~3|`)')
    df['INVOICE_NUMBER_FORMAT'] = df['INVOICE_NUMBER'].apply(lambda x: re.sub(invoice_pattern, '', x))

    df['INVOICE_AMOUNT_ABS'] = df['INVOICE_AMOUNT'].apply(lambda x: abs(x))

    df['INVOICE_TYPE'] = df['INVOICE_TYPE'].apply(lambda x: x.strip().upper())
    df = df.query('INVOICE_TYPE == "IN" or INVOICE_TYPE == "DM"')


    df['INVOICE_STATUS'] = df['INVOICE_STATUS'].apply(lambda x: x.strip().upper())
    df = df.query('INVOICE_STATUS == "PAID" or INVOICE_STATUS == "VOIDED"')

    df = df.query('SUPPLY_GROUP_ID not in ["Employee", "EMPLOYEE", "Ex Empl/Rel", "Retiree"]')
    df = df.query('INVOICE_AMOUNT != 0')

    def remove_suffixes(invoice_number):
        group = ['VD1', 'VD2', 'VD3', 'CR1', 'CR2', 'CR3', 'CR']
        if any([g == invoice_number[-3:] for g in group]):
            return invoice_number[:-3]
        elif 'CR' in invoice_number:
            return invoice_number.replace("CR", "")
        else:
            return invoice_number

    df['INVOICE_NUMBER_FORMAT'] = df['INVOICE_NUMBER_FORMAT'].apply(lambda x: remove_suffixes(x))

    supplier_pattern = re.compile(r'(-| |#|!|@|\*|\$|%|\^|&|\"|/|\.|\+|\||\\|\'\'|\)|\(|_|\<|\>|\?|,|\[|\]|;|:|`)')
    # st = "- #!@*hello$%^&\"/\\.+()_<>?,[];:`"
    # re.sub(supplier_pattern, '', st)
    df['SUPPLIER_NAME_FORMAT'] = df['SUPPLIER_NAME'].apply(lambda x: re.sub(supplier_pattern, '', x))

    return df