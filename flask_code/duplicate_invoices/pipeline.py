from sklearn.pipeline import Pipeline
from sklearn.dummy import DummyClassifier
from duplicate_invoices.processing import preprocessors as pp
from duplicate_invoices.config import config, logging_config
from duplicate_invoices.model.duplicate_extraction import DuplicateExtract
import logging

_logger = logging.getLogger(__name__)
file_handler = logging.FileHandler(config.LOGS_DIR / f"{__name__}.txt")
formatter = logging_config.FORMATTER
file_handler.setFormatter(formatter)
_logger.addHandler(file_handler)


pipe = Pipeline(
    [
        (
            "insert_primary_key",
            pp.InsertPrimaryKey(variables=config.PRIMARY_KEY_VARIABLES),
        ),
        (
            "format_invoice_number",
            pp.FormatInvoiceNumber(),
        ),
        (
            "format_supplier_name",
            pp.FormatSupplierName(),
        ),
        (
            "other_format_filter",
            pp.OtherFormatAndFilter(),
        ),
        (
            "duplicate_invoice_main_model",
            DuplicateExtract()
        )
    ]
)