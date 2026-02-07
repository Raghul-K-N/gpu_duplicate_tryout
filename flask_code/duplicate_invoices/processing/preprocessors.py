from sklearn.base import BaseEstimator, TransformerMixin
from duplicate_invoices.config import logging_config, config
import logging
import pandas as pd
import re


_logger = logging.getLogger(__name__)
file_handler = logging.FileHandler(config.LOGS_DIR / f"{__name__}.txt")
formatter = logging_config.FORMATTER
file_handler.setFormatter(formatter)
_logger.addHandler(file_handler)


class InsertPrimaryKey(BaseEstimator, TransformerMixin):
    """Insert a Primary Key into the dataset"""

    def __init__(self, variables=None) -> None:
        if not isinstance(variables, list):
            self.variables = [variables]
        else:
            self.variables = variables

    def fit(self, X: pd.DataFrame, y: pd.Series = None) -> "InsertPrimaryKey":
        """Fit statement to accomodate the sklearn pipeline."""

        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Apply the transforms to the dataframe."""

        if config.MODE == 'AMC':
            X['PrimaryKeySimple'] = X.index
            return X

        temp = X[self.variables[0]].astype(str)
        for v in self.variables[1:-1]:
            temp += X[v].astype(str) + '-'
        temp += X[self.variables[-1]].astype(str)

        X['PrimaryKey'] = temp

        X['PrimaryKeySimple'] = X.groupby("PrimaryKey").ngroup()
        X['PrimaryKeySimple'] = X['PrimaryKeySimple'].astype(str)

        return X


class FormatInvoiceNumber(BaseEstimator, TransformerMixin):
    """Pre-processing for the Invoice Number"""

    def fit(self, X: pd.DataFrame, y: pd.Series = None) -> "FormatInvoiceNumber":
        """Fit statement to accomodate the sklearn pipeline."""

        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Apply the transforms to the dataframe."""

        X[config.INVOICE_NUMBER_COLUMN] = X[config.INVOICE_NUMBER_COLUMN].apply(lambda x: x.strip().upper())

        # invoice_pattern = re.compile(r'(\'\'2|\'\'1|~1|~2|~3|`)')
        pattern_text = r'(...|\'\'2|\'\'1|~1|~2|~3|`|#|&|\||' + \
        r'$|\'\'|.001...|.002...|.003...|.004...|.005...|.006...|.007...|.008...|.009...|.010...|' + \
        r'.0001...|.0002...|.0003...|.0004...|.0005...|.0006...|.0007...|.0008...|.0009...|.0010...|' + \
        r'.001A...|.002A...|.003A...|.004A...|.005A...|.006A...|.007A...|.008A...|.009A...|.010A...|' + \
        r'.001B...|.002B...|.003B...|.004B...|.005B...|.006B...|.007B...|.008B...|.009B...|.010B...|' + \
        r'A.1-|A.2-|A.3-|A.4-|A.5-|A.6-|A.7-|A.8-|A.9-|A.10-|' + \
        r'A.01-|A.02-|A.03-|A.04-|A.05-|A.06-|A.07-|A.08-|A.09-|A.010-|' + \
        r'B.1-|B.2-|B.3-|B.4-|B.5-|B.6-|B.7-|B.8-|B.9-|B.10-|' + \
        r'B.01-|B.02-|B.03-|B.04-|B.05-|B.06-|B.07-|B.08-|B.09-|B.010-|' + \
        r'C.1-|C.2-|C.3-|C.4-|C.5-|C.6-|C.7-|C.8-|C.9-|C.10-|' + \
        r'C.01-|C.02-|C.03-|C.04-|C.05-|C.06-|C.07-|C.08-|C.09-|C.010-|' + \
        r'.A-1...|.A-2...|.A-3...|.A-4...|.A-5...|.A-6...|.A-7...|.A-8...|.A-9...|.A-10...|' + \
        r'.1A...|.2A...|.3A...|.4A...|.5A...|.6A...|.7A...|.8A...|.9A...|.10A...|' + \
        r'.1B...|.2B...|.3B...|.4B...|.5B...|.6B...|.7B...|.8B...|.9B...|.10B...|' + \
        r'.1C...|.2C...|.3C...|.4C...|.5C...|.6C...|.7C...|.8C...|.9C...|.10C...|' + \
        r'.1D...|.2D...|.3D...|.4D...|.5D...|.6D...|.7D...|.8D...|.9D...|.10D...|' + \
        r'.1E...|.2E...|.3E...|.4E...|.5E...|.6E...|.7E...|.8E...|.9E...|.10E...|' + \
        r'.1F...|.2F...|.3F...|.4F...|.5F...|.6F...|.7F...|.8F...|.9F...|.10F...|'+ \
        r'-20...|-40...|-22...|-23...|-24...|-25...|-26...|-27...|-28...|-29...|-30...|'+ \
        r'-31...|-32...|-33...|-34...|-35...|-36...|-37...|-38...|-39...|' + \
        r'-A...|-B...|-C...|-D...|-E...|-F...|-G...|-H...|-INV...|' + \
        r'-IN...|A...|B...|C...|D...|E...|F...|G...|H...|I...|' + \
        r'J...|-...|--R...|-S-...|-0...|-1...|-2...|-3...|-4...|-5...|-6...|' + \
        r'-7...|-8...|-9...|-00...|-01...|-02...|-03...|-04...|-05...|-06...|' + \
        r'-07...|-08...|-09...|-10...|-001...|-002...|-003...|-004...|-005...|' + \
        r'-006...|-007...|-008...|-009...|-010...|-0001...|-0002...|-0003...|-0004...|' + \
        r'-0005...|-0006...|-0007...|-0008...|-0009...|-0010...|.1...|' + \
        r'.2...|.3...|.4...|.5...|.6..|.7...|.8...|.9...|.10...)'

        prev_c = pattern_text[0]
        final_text = ""
        for c in pattern_text[1:]:
            if prev_c == '.':
                final_text += '\\'
            final_text += prev_c
            prev_c = c
        final_text += prev_c

        invoice_pattern = re.compile(final_text)
        X['INVOICE_NUMBER_FORMAT'] = X[config.INVOICE_NUMBER_COLUMN].apply(lambda x: re.sub(invoice_pattern, '', x))

        def remove_suffixes(invoice_number):
            group = ['VD1', 'VD2', 'VD3', 'CR1', 'CR2', 'CR3', 'CR']
            if any([g == invoice_number[-3:] for g in group]):
                return invoice_number[:-3]
            elif 'CR' in invoice_number:
                return invoice_number.replace("CR", "")
            else:
                return invoice_number

        X['INVOICE_NUMBER_FORMAT'] = X['INVOICE_NUMBER_FORMAT'].apply(lambda x: remove_suffixes(x))
        X['INVOICE_NUMBER_FORMAT'] = X['INVOICE_NUMBER_FORMAT'].apply(lambda x: x.lstrip("0"))  # Strip leading zeros from invoice number columns

        return X


class FormatSupplierName(BaseEstimator, TransformerMixin):
    """Pre-processing for the Supplier Name"""


    def fit(self, X: pd.DataFrame, y: pd.Series = None) -> "FormatInvoiceNumber":
        """Fit statement to accomodate the sklearn pipeline."""

        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Apply the transforms to the dataframe."""
        # X['SUPPLIER_NAME'] = X['SUPPLIER_NAME'].apply(lambda x: x.strip().upper())
        if config.SUPPLIER_NAME_COLUMN is None:
            return X
        X[config.SUPPLIER_NAME_COLUMN] = X[config.SUPPLIER_NAME_COLUMN].apply(lambda x: x.strip() 
                                                                              if isinstance(x, str) else x)

        supplier_pattern = re.compile(r'(-| |#|!|@|\*|\$|%|\^|&|\"|/|\.|\+|\||\\|\'\'|\)|\(|_|\<|\>|\?|,|\[|\]|;|:|`)')
        X['SUPPLIER_NAME_FORMAT'] = X[config.SUPPLIER_NAME_COLUMN].apply(lambda x: re.sub(supplier_pattern, '', x)
                                                                         if isinstance(x, str) else x)

        return X


class OtherFormatAndFilter(BaseEstimator, TransformerMixin):
    """Some other formatting and filtering"""

    def fit(self, X: pd.DataFrame, y: pd.Series = None) -> "FormatInvoiceNumber":
        """Fit statement to accomodate the sklearn pipeline."""

        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Apply the transforms to the dataframe."""
        X['INVOICE_AMOUNT_ABS'] = X.loc[:, config.INVOICE_AMOUNT_COLUMN].apply(lambda x: abs(x))

        if config.INVOICE_TYPE_COLUMN is not None:
            X[config.INVOICE_TYPE_COLUMN] = X.loc[:, config.INVOICE_TYPE_COLUMN].apply(lambda x: x.strip().upper())
            X = X.query(f'{config.INVOICE_TYPE_COLUMN} == "IN" or {config.INVOICE_TYPE_COLUMN} == "DM"').copy(deep=True)

        if config.INVOICE_STATUS_COLUMN is not None:
            X[config.INVOICE_STATUS_COLUMN] = X.loc[:, config.INVOICE_STATUS_COLUMN].apply(lambda x: x.strip().upper())
            X = X.query(f'{config.INVOICE_STATUS_COLUMN} == "PAID" or {config.INVOICE_STATUS_COLUMN} == "VOIDED"').copy(deep=True)

        if config.SUPPLY_GROUP_ID_COLUMN is not None:
            X = X.query(f'{config.SUPPLY_GROUP_ID_COLUMN} not in ["Employee", "EMPLOYEE", "Ex Empl/Rel", "Retiree"]').copy(deep=True)
        # X = X.query(f'{self.invoice_amount_column} != 0').copy(deep=True)
        X = X[X[config.INVOICE_AMOUNT_COLUMN] != 0].copy(deep=True)

        return X
