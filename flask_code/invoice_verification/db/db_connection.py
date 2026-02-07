# db/db_connection.py
"""
Database connection management and utility functions
Supports quarterly-sharded tables and future multiprocessing
"""

import pandas as pd
import time
import math
import json
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.mysql import insert as mysql_insert
# from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.logger.logger import log_message
from typing import Dict, List
from invoice_verification.Schemas.invoice_processing_result import InvoiceProcessingResult
from invoice_verification.Schemas.invoice_verification_result import InvoiceVerificationResult
from invoice_verification.Schemas.invoice_param_config_result import InvoiceParamConfigResult
from invoice_verification.Schemas.ui_invoice_flat_result import UIInvoiceFlatResult
from invoice_verification.Schemas.invoice_attachments_result import InvoiceAttachmentsResult


# Module-level engine/session factory (initialized per-process by init_db)
_engine = None
_SessionFactory = None
_QUARTERLY_MODELS_CACHE: Dict[str, Dict] = {}


# INITIALIZATION AND CLEANUP
def init_db(db_uri: str, use_null_pool: bool = False, pool_size: int = 5, 
            max_overflow: int = 10, pool_recycle: int = 3600, echo: bool = False):
    """
    Initialize engine and SessionFactory in the current process.
    Call this in the main process and in Pool initializer for multiprocessing.
    Args:
        db_uri: Database connection URI
        use_null_pool: If True, use NullPool (no connection pooling)
        pool_size: Number of connections to maintain
        max_overflow: Max additional connections
        pool_recycle: Recycle connections after this many seconds
        echo: If True, log all SQL statements
    """
    global _engine, _SessionFactory
    
    if use_null_pool:
        _engine = create_engine(db_uri, poolclass=NullPool, pool_pre_ping=True, echo=echo)
    else:
        _engine = create_engine(
            db_uri,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True,
            pool_recycle=pool_recycle,
            echo=echo
        )
    # Prevent attribute expiration on commit to avoid DetachedInstanceError
    _SessionFactory = sessionmaker(bind=_engine, expire_on_commit=False)
    
    import os
    log_message(f"db_conn: engine and SessionFactory initialized in pid={os.getpid()}")


def dispose_engine():
    """Dispose engine in this process (call at process exit if needed)"""
    global _engine, _SessionFactory
    
    try:
        if _engine:
            _engine.dispose()
    finally:
        _engine = None
        _SessionFactory = None
        import os
        log_message(f"db_conn: engine disposed in pid={os.getpid()}")


def get_engine():
    """Get the current engine instance"""
    global _engine
    if _engine is None:
        raise RuntimeError("Engine not initialized. Call init_db() first.")
    return _engine


# SESSION MANAGEMENT
@contextmanager
def get_session():
    """
    Context manager that yields a session.
    Commits on success, rolls back on exception, always closes.
    Usage:
        with get_session() as session:
            session.add(obj)
            # commit happens automatically
    """
    global _SessionFactory
    
    if _SessionFactory is None:
        raise RuntimeError("SessionFactory not initialized. Call init_db() in this process.")
    
    session = _SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# QUARTERLY TABLE OPERATIONS
def create_quarterly_tables_if_not_exist(quarter_label: str, models_dict: Dict):
    """
    Create quarterly tables if they don't exist.
    Args:
        quarter_label: e.g., 'q1_2025'
        models_dict: Dict of model classes keyed by table type
                    {'processing': ProcessingModel, 'verification': VerificationModel, ...}
    """
    engine = get_engine()
    
    for table_type, model_class in models_dict.items():
        try:
            model_class.__table__.create(bind=engine, checkfirst=True)
            log_message(f"Table {model_class.__tablename__} created/verified for quarter {quarter_label}")
        except SQLAlchemyError as e:
            log_message(f"Error creating table {model_class.__tablename__}: {e}",error_logger=True)
            raise

def read_table(table_name: str) -> pd.DataFrame:
    """
    Read entire table into a DataFrame.
    Args:
        table_name: Name of the table to read
    Returns:
        pd.DataFrame with table data
    """
    engine = get_engine()
    try:
        with engine.connect() as connection:
            query = text(f"SELECT * FROM {table_name};")
            df = pd.read_sql(query, connection)
            log_message(f"Read {df.shape[0]} rows from table {table_name}")
            return df
    except SQLAlchemyError as e:
        log_message(f"Error reading table {table_name}: {e}", error_logger=True)
        raise

def setup_quarterly_tables(quarters: List[str]):
    """
    Function to:
    1. Create tables if they don't exist
    2. Initialize and global dict to cache models
    """
    global _QUARTERLY_MODELS_CACHE  # Add global declaration
    
    try:
        # Create tables and cache models
        log_message(f"Setting up tables for quarters: {quarters}")
        
        # Create tables and cache models in one pass
        for quarter_label in quarters:
            # Get models for this quarter
            from invoice_verification.db.models import get_quarterly_models
            models_dict = get_quarterly_models(quarter_label)
            
            create_quarterly_tables_if_not_exist(quarter_label, models_dict)
            
            models_dict.pop('feedback_history')
            # Cache the models in global dict
            _QUARTERLY_MODELS_CACHE[quarter_label] = models_dict
            
            log_message(f"Quarter {quarter_label} setup complete")
        
    except Exception as e:
        log_message(f"Error in setup_quarterly_tables: {e}", error_logger=True)
        clear_models_cache()
        raise


def get_quarterly_models_from_dict(quarter_label: str) -> Dict:
    """
    Get cached models for a specific quarter.
    Args:
        quarter_label: Quarter label (e.g., 'q1_2025')
    Returns:
        Dict of {table_type: ModelClass}
    """
    global _QUARTERLY_MODELS_CACHE

    quarter_label = str(quarter_label).lower().replace("-", "_")
    
    if quarter_label not in _QUARTERLY_MODELS_CACHE:
        log_message(f"Quarter {quarter_label} not in cache, creating on-demand", error_logger=True)
        from invoice_verification.db.models import get_quarterly_models
        _QUARTERLY_MODELS_CACHE[quarter_label] = get_quarterly_models(quarter_label)
    
    return _QUARTERLY_MODELS_CACHE[quarter_label]


def clear_models_cache():
    """Clear the models cache (useful for testing or batch completion)"""
    global _QUARTERLY_MODELS_CACHE
    _QUARTERLY_MODELS_CACHE = {}
    log_message("Model cache cleared")


# INVOICE OPERATIONS (Session-per-invoice)
def insert_invoice_processing_and_verification(
    processing_dict: Dict,
    verification_dict: Dict,
    processing_model,
    verification_model,
    retry_on_deadlock: int = 2
) -> int:
    """
    Insert or update invoice processing and verification records in a single transaction.
    Uses account_document_id as unique key for upsert logic.
    Args:
        processing_dict: Dict of processing table fields (must include account_document_id)
        verification_dict: Dict of verification table fields (without processing_id)
        processing_model: Processing table model class for this quarter
        verification_model: Verification table model class for this quarter
        retry_on_deadlock: Number of retry attempts on deadlock
    Returns:
        int: The processing_id (invoice_id)
    """
    attempt = 0
    
    while True:
        try:
            with get_session() as session:
                # Sanitize parameters before creating model instance
                sanitized_processing = sanitize_parameters(processing_dict)
                sanitized_verification = sanitize_parameters(verification_dict)
                
                # Get account_document_id for upsert check
                account_doc_id = sanitized_processing.get('account_document_id')
                if not account_doc_id:
                    raise ValueError("account_document_id is required for upsert")
                
                # Check if processing record exists
                existing_processing = session.query(processing_model).filter_by(
                    account_document_id=account_doc_id
                ).first()
                
                if existing_processing:
                    # UPDATE existing processing record
                    for key, value in sanitized_processing.items():
                        if key not in ['created_at', 'created_date']:  # Preserve creation timestamp
                            setattr(existing_processing, key, value)
                    session.flush()
                    processing_id = existing_processing.id
                    log_message(f"Updated existing processing record with invoice_id: {processing_id}")
                else:
                    # INSERT new processing record
                    processing_record = processing_model(**sanitized_processing)
                    session.add(processing_record)
                    session.flush()  # Get ID
                    processing_id = processing_record.id
                    log_message(f"Inserted new processing record with invoice_id: {processing_id}")
                
                # Check if verification record exists
                existing_verification = session.query(verification_model).filter_by(
                    account_document_id=account_doc_id
                ).first()
                
                if existing_verification:
                    # UPDATE existing verification record
                    for key, value in sanitized_verification.items():
                        if key not in ['created_at', 'created_date']:
                            setattr(existing_verification, key, value)
                    # Update processing_id FK in case it changed
                    existing_verification.processing_id = processing_id
                    log_message(f"Updated existing verification record for account_document_id: {account_doc_id}")
                else:
                    # INSERT new verification record
                    # Note: account_document_id is already in sanitized_verification
                    sanitized_verification['processing_id'] = processing_id
                    verification_record = verification_model(**sanitized_verification)
                    session.add(verification_record)
                    log_message(f"Inserted new verification record for account_document_id: {account_doc_id}")
            
            return processing_id
        
        except SQLAlchemyError as e:
            attempt += 1
            log_message(f"Upsert failed (attempt {attempt}): {e}", error_logger=True)
            
            if attempt > retry_on_deadlock:
                raise
            
            time.sleep(0.2 * attempt)


def insert_invoice_param_config(
    param_config_list: List[Dict],
    param_config_model,
    retry_on_deadlock: int = 2
) -> bool:
    """
    Insert or update parameter configuration records for an invoice.
    Uses composite key (invoice_id, param_code) for upsert logic.
    Args:
        param_config_list: List of param config dicts (must include invoice_id and param_code)
        param_config_model: ParamConfig table model class
        retry_on_deadlock: Number of retry attempts
    Returns:
        bool: True if successful
    """
    if not param_config_list:
        return True
    
    attempt = 0
    
    while True:
        try:
            with get_session() as session:
                insert_count = 0
                update_count = 0
                
                for config_dict in param_config_list:
                    sanitized_config = sanitize_parameters(config_dict)
                    
                    # Get composite key values for upsert check
                    invoice_id = sanitized_config.get('invoice_id')
                    param_code = sanitized_config.get('param_code')
                    
                    if not invoice_id or not param_code:
                        raise ValueError(f"invoice_id and param_code are required for upsert: {sanitized_config}")
                    
                    # Check if param config record exists
                    existing_config = session.query(param_config_model).filter_by(
                        invoice_id=invoice_id,
                        param_code=param_code
                    ).first()
                    
                    if existing_config:
                        # UPDATE existing record - skip None values to avoid NOT NULL constraint violations
                        log_message(f"Param config exists for invoice_id: {invoice_id}, param_code: {param_code}, values to be updated: {sanitized_config}")
                        for key, value in sanitized_config.items():
                            if key not in ['created_at', 'created_date'] and value is not None:
                                setattr(existing_config, key, value)
                        update_count += 1
                        log_message(f"Updated param config for invoice_id: {invoice_id}, param_code: {param_code}")
                    else:
                        # INSERT new record
                        config_record = param_config_model(**sanitized_config)
                        session.add(config_record)
                        insert_count += 1
                        log_message(f"Inserted param config for invoice_id: {invoice_id}, param_code: {param_code}, values: {sanitized_config}")
            
            log_message(f"Param config: inserted {insert_count}, updated {update_count} records")
            return True
        
        except SQLAlchemyError as e:
            attempt += 1
            log_message(f"Param config upsert failed (attempt {attempt}): {e}", error_logger=True)
            
            if attempt > retry_on_deadlock:
                raise
            
            time.sleep(0.2 * attempt)


def insert_invoice_attachments(
    attachments_list: List[Dict],
    attachments_model,
    retry_on_deadlock: int = 2
) -> bool:
    """
    Insert attachment records for an invoice (insert-only, no update).
    Checks if attachment already exists using (invoice_id, file_path) to avoid duplicates.
    
    Args:
        attachments_list: List of attachment dicts with {invoice_id, file_path, file_type}
        attachments_model: Attachments table model class
        retry_on_deadlock: Number of retry attempts
    
    Returns:
        bool: True if successful
    """
    if not attachments_list:
        return True
    
    attempt = 0
    
    while True:
        try:
            with get_session() as session:
                insert_count = 0
                skip_count = 0
                
                for attachment_dict in attachments_list:
                    sanitized_attachment = sanitize_parameters(attachment_dict)
                    
                    # Get unique key values for existence check
                    invoice_id = sanitized_attachment.get('invoice_id')
                    file_path = sanitized_attachment.get('file_path')
                    
                    if not invoice_id or not file_path:
                        raise ValueError(f"invoice_id and file_path are required: {sanitized_attachment}")
                    
                    # Check if attachment already exists
                    existing_attachment = session.query(attachments_model).filter_by(
                        invoice_id=invoice_id,
                        file_path=file_path
                    ).first()
                    
                    if not existing_attachment:
                        # INSERT only if not exists
                        attachment_record = attachments_model(**sanitized_attachment)
                        session.add(attachment_record)
                        insert_count += 1
                        log_message(f"Inserted attachment for invoice_id: {invoice_id}, file_path: {file_path}, values: {attachment_record}")
                    else:
                        # Skip duplicate
                        skip_count += 1
                        log_message(f"Skipped duplicate attachment for invoice_id: {invoice_id}, file_path: {file_path}")
            
            log_message(f"Attachments: inserted {insert_count}, skipped {skip_count} duplicate records")
            return True
        
        except SQLAlchemyError as e:
            attempt += 1
            log_message(f"Attachments insert failed (attempt {attempt}): {e}", error_logger=True)
            
            if attempt > retry_on_deadlock:
                raise
            
            time.sleep(0.2 * attempt)


def insert_ui_invoice_flat(
    ui_flat_dict: Dict,
    ui_flat_model,
    retry_on_deadlock: int = 2
) -> bool:
    """
    Insert or update UI flat record for an invoice.
    Uses invoice_id as unique key for upsert logic.
    Args:
        ui_flat_dict: Dict of UI flat table fields (must include invoice_id)
        ui_flat_model: UIFlat table model class
        retry_on_deadlock: Number of retry attempts
    Returns:
        bool: True if successful
    """
    attempt = 0
    
    while True:
        try:
            with get_session() as session:
                sanitized_ui_flat = sanitize_parameters(ui_flat_dict)
                
                # Get invoice_id for upsert check
                invoice_id = sanitized_ui_flat.get('invoice_id')
                if not invoice_id:
                    raise ValueError("invoice_id is required for upsert")
                
                # Check if UI flat record exists
                existing_ui_flat = session.query(ui_flat_model).filter_by(
                    invoice_id=invoice_id
                ).first()
                
                if existing_ui_flat:
                    log_message(f"UI flat record exists for invoice_id: {invoice_id}, values to be updated: {sanitized_ui_flat}")
                    # UPDATE existing record
                    for key, value in sanitized_ui_flat.items():
                        if key not in ['created_at', 'created_date']:
                            setattr(existing_ui_flat, key, value)
                    log_message(f"Updated UI flat record for invoice_id: {invoice_id}")
                else:
                    # INSERT new record
                    ui_flat_record = ui_flat_model(**sanitized_ui_flat)
                    log_message(f"UI flat record does not exist for invoice_id: {invoice_id}, values to be inserted: {sanitized_ui_flat}")
                    session.add(ui_flat_record)
                    log_message(f"Inserted UI flat record for invoice_id: {invoice_id}")
            
            return True
        
        except SQLAlchemyError as e:
            attempt += 1
            log_message(f"UI flat upsert failed (attempt {attempt}): {e}", error_logger=True)
            
            if attempt > retry_on_deadlock:
                raise
            
            time.sleep(0.2 * attempt)


def insert_complete_invoice_data(
    processing_result: InvoiceProcessingResult,  
    verification_result: InvoiceVerificationResult,
    param_config_result: InvoiceParamConfigResult,
    ui_flat_result: UIInvoiceFlatResult,
    attachments_result: InvoiceAttachmentsResult,
    quarter_label: str,
    retry_on_deadlock: int = 2
)  -> int:
    """
    Insert complete invoice data across all tables using result objects.
    Dynamically gets models based on quarter_label.
    
    Args:
        processing_result: InvoiceProcessingResult object
        verification_result: InvoiceVerificationResult object
        param_config_result: InvoiceParamConfigResult object
        ui_flat_result: UIInvoiceFlatResult object
        attachments_result: InvoiceAttachmentsResult object
        quarter_label: Quarter label (e.g., 'q1_2025') from SAP date_label
        retry_on_deadlock: Number of retry attempts
    
    Returns:
        int: The auto-generated processing_id
    """

    # Get models for this quarter
    models_dict = get_quarterly_models_from_dict(quarter_label)
    
    # Convert result objects to dicts
    processing_dict = processing_result.to_db_dict()
    verification_dict = verification_result.to_db_dict()
    param_config_list = param_config_result.to_db_list()  # Returns list of dicts
    ui_flat_dict = ui_flat_result.to_db_dict()
    attachments_list = attachments_result.to_db_list()  # Returns list of dicts
    log_message(f"Preparing to insert complete invoice data for quarter: {quarter_label}")
    log_message("")  # Empty line for readability
    log_message(f"Processing dict : {processing_dict}")
    log_message(f"")  # Empty line for readability
    log_message(f"Verification dict : {verification_dict}")
    log_message(f"")  # Empty line for readability
    log_message(f"Param config list : {param_config_list}")
    log_message(f"")  # Empty line for readability
    log_message(f"UI flat dict : {ui_flat_dict}")
    log_message(f"")  # Empty line for readability
    log_message(f"Attachments list : {attachments_list}")

    # Step 1: Insert processing and verification
    processing_id = insert_invoice_processing_and_verification(
        processing_dict=processing_dict,
        verification_dict=verification_dict,
        processing_model=models_dict['processing'],
        verification_model=models_dict['verification'],
        retry_on_deadlock=retry_on_deadlock
    )
    
    # Step 2: Add processing_id to dependent tables
    for config in param_config_list:
        config['invoice_id'] = processing_id
    
    ui_flat_dict['invoice_id'] = processing_id
    
    for attachment in attachments_list:
        attachment['invoice_id'] = processing_id
    
    # Step 3: Insert param config
    if param_config_list:
        insert_invoice_param_config(
            param_config_list=param_config_list,
            param_config_model=models_dict['param_config'],
            retry_on_deadlock=retry_on_deadlock
        )
    
    # Step 4: Insert UI flat
    insert_ui_invoice_flat(
        ui_flat_dict=ui_flat_dict,
        ui_flat_model=models_dict['ui_flat'],
        retry_on_deadlock=retry_on_deadlock
    )
    
    # Step 5: Insert attachments
    if attachments_list:
        insert_invoice_attachments(
            attachments_list=attachments_list,
            attachments_model=models_dict['attachments'],
            retry_on_deadlock=retry_on_deadlock
        )
    
    log_message(f"Successfully inserted complete invoice data with processing_id: {processing_id}")
    return processing_id


FIELD_ANOMALY_MAP = {
    'invoice_is_attached': 'invoice_is_attached_anomaly',
    'invoice_number': 'invoice_number_anomaly',
    'gl_account_number': 'gl_account_number_anomaly',
    'invoice_amount': 'invoice_amount_anomaly',
    'invoice_currency': 'invoice_currency_anomaly',
    'vendor_name': 'vendor_name_and_address_anomaly',
    'vendor_address': 'vendor_name_and_address_anomaly',
    'legal_entity_name': 'legal_entity_name_and_address_anomaly',
    'legal_entity_address': 'legal_entity_name_and_address_anomaly',
    'payment_terms': 'payment_terms_anomaly',
    'invoice_date': 'invoice_date_anomaly',
    'text_info': 'text_info_anomaly',
    'legal_requirement': 'legal_requirement_anomaly',
    'doa': 'doa_anomaly',
    'udc': 'udc_anomaly',
    'transaction_type': 'transaction_type_anomaly',
    'vat_tax_code': 'vat_tax_code_anomaly',
    'service_invoice_confirmation': 'service_invoice_confirmation_anomaly',
    'bank_name': 'vendor_banking_details_anomaly',
    'bank_account_number': 'vendor_banking_details_anomaly',
    'bank_account_holder_name': 'vendor_banking_details_anomaly',
    'payment_method': 'payment_method_anomaly',
    'invoice_receipt_date': 'invoice_receipt_date_anomaly'
}

FIELD_PARAM_CODE_MAP = {
    'invoice_is_attached': 'INVOICE_IS_ATTACHED',
    'invoice_number': 'INVOICE_NUMBER',
    'gl_account_number': 'GL_ACCOUNT_NUMBER',
    'invoice_amount': 'INVOICE_AMOUNT',
    'invoice_currency': 'INVOICE_CURRENCY',
    'vendor_name': 'VENDOR_DETAILS',
    'vendor_address': 'VENDOR_DETAILS',
    'legal_entity_name': 'LEGAL_ENTITY_NAME_AND_ADDRESS',
    'legal_entity_address': 'LEGAL_ENTITY_NAME_AND_ADDRESS',
    'payment_terms': 'PAYMENT_TERMS',
    'invoice_date': 'INVOICE_DATE',
    'text_info': 'TEXT_INFO',
    'legal_requirement': 'LEGAL_REQUIREMENT',
    'doa': 'DOA',
    'udc': 'UDC',
    'transaction_type': 'TRANSACTION_TYPE',
    'vat_tax_code': 'VAT_TAX_CODE',
    'service_invoice_confirmation': 'SERVICE_INVOICE_CONFIRMATION',
    'bank_name': 'VENDOR_BANKING_DETAILS',
    'bank_account_number': 'VENDOR_BANKING_DETAILS',
    'bank_account_holder_name': 'VENDOR_BANKING_DETAILS',
    'payment_method': 'PAYMENT_METHOD',
    'invoice_receipt_date': 'INVOICE_RECEIPT_DATE'
}

def get_param_mappings():
    """
    Fetch parameter-to-field mappings with a single join query.

    Returns:
        Tuple of two dicts:
        - anomaly_map: field_name → anomaly_column_name
        - field_map: field_name → param_code
    """
    engine = get_engine()
    anomaly_map, field_map = {}, {}

    query = """
        SELECT 
            pfm.field_name,
            pd.param_code,
            pd.anomaly_column_name
        FROM param_field_map pfm
        JOIN param_definition pd 
            ON pfm.param_code = pd.param_code
        WHERE pd.status = 1
            AND pfm.field_name IS NOT NULL
            AND pd.param_code IS NOT NULL
    """
    try:
        with engine.connect() as conn:
            for row in conn.execute(text(query)):
                field_name = row[0]  # field_name
                param_code = row[1]  # param_code
                anomaly_col = row[2]  # anomaly_column_name

                # Defensive checks — ignore any None or duplicate entries
                if not field_name or not param_code:
                    continue
                if field_name not in field_map:
                    field_map[field_name] = param_code
                    anomaly_map[field_name] = anomaly_col

        return anomaly_map, field_map

    except Exception as e:
        from invoice_verification.logger.logger import log_message
        log_message(f"Error fetching param mappings from DB: {e}", error_logger=True)
        return FIELD_ANOMALY_MAP, FIELD_PARAM_CODE_MAP






from sqlalchemy import text
import math
import json

# Fields that are stored as JSON columns in the database
# These should keep dict/list as-is - SQLAlchemy JSON column handles serialization
JSON_COLUMN_FIELDS = {
    'supporting_fields',      # InvoiceParamConfig and UIInvoiceFlat
    'editable_fields',        # UIInvoiceFlat
    'highlightable_fields',   # UIInvoiceFlat
    'validation_methods',     # UIInvoiceFlat
    'extracted_value',        # If used anywhere
    'supporting_details'      # Parameter validation results
}

def sanitize_parameters(params):
    """
    Convert empty strings, empty lists, and NaN values to None for database insertion.
    Handles numeric conversion and data type validation.
    
    For JSON column fields (supporting_fields, editable_fields, etc.), 
    dict/list values are kept as-is since SQLAlchemy's JSON column handles serialization.
    """
    sanitized = {}
    for key, value in params.items():
        # Convert NaN values to None (check for float NaN and pandas NaN)
        if value is None or (isinstance(value, (float, int)) and math.isnan(value)):
            sanitized[key] = None
        # Convert empty strings to None
        elif value == '':
            sanitized[key] = None
        # Handle dict/list values
        elif isinstance(value, (dict, list)):
            if key in JSON_COLUMN_FIELDS:
                # Keep as dict/list for JSON columns - SQLAlchemy handles serialization
                # Convert empty dict/list to None
                sanitized[key] = value if value else None
            else:
                # Non-JSON column field with dict/list - this is a data preparation issue
                log_message(f"Warning: Field '{key}' contains {type(value).__name__} but is not a JSON column: {value}")
                # Convert to JSON string as fallback
                try:
                    sanitized[key] = json.dumps(value) if value else None
                except (TypeError, ValueError):
                    sanitized[key] = str(value)
        # Convert string numbers to appropriate types
        elif isinstance(value, str) and key in ['invoice_amount', 'vat_amount']:
            try:
                # Remove commas and convert to float
                sanitized[key] = float(value.replace(',', '')) if value else None
            except (ValueError, AttributeError):
                sanitized[key] = None
        else:
            sanitized[key] = value
    return sanitized



def mysql_upsert_method(table, conn, keys, data_iter):
    """
    Custom method for pandas to_sql() that performs MySQL UPSERT.
    Uses INSERT ... ON DUPLICATE KEY UPDATE for efficient bulk upsert.
    
    Args:
        table: SQLAlchemy Table object
        conn: Database connection
        keys: Column names
        data_iter: Iterator of data rows
    """
    
    
    # Convert iterator to list of dicts
    data = [dict(zip(keys, row)) for row in data_iter]
    
    if not data:
        return
    
    # Build INSERT statement
    insert_stmt = mysql_insert(table.table).values(data)
    
    # Build UPDATE clause (don't update CREATED_AT timestamp on duplicate)
    excluded_from_update = {'CREATED_AT', 'CREATED_DATE'}
    
    update_dict = {
        col.name: col 
        for col in insert_stmt.inserted 
        if col.name not in excluded_from_update
    }
    
    # Create ON DUPLICATE KEY UPDATE statement
    upsert_stmt = insert_stmt.on_duplicate_key_update(**update_dict)
    
    # Execute
    conn.execute(upsert_stmt)


def create_zblock_account_document_table(table_name: str):
    """
    Create z_block_account_document_flat table with specified name."""
    engine = get_engine()

    table_name = str(table_name).lower().replace("-", "_").strip()
    log_message(f"Creating table: {table_name}")
    create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
            ACCOUNT_DOC_ID INT DEFAULT NULL,
            CLIENT VARCHAR(10) DEFAULT NULL,
            COMPANY_CODE VARCHAR(45) DEFAULT NULL,
            REGION VARCHAR(50) DEFAULT NULL,
            ENTRY_ID VARCHAR(50) DEFAULT NULL,
            FISCAL_YEAR VARCHAR(10) DEFAULT NULL,
            VENDORCODE VARCHAR(45) DEFAULT NULL,
            VENDOR_NAME VARCHAR(255) DEFAULT NULL,
            VENDOR_ADDRESS VARCHAR(500) DEFAULT NULL,
            DOCUMENT_TYPE VARCHAR(10) DEFAULT NULL,
            INVOICE_DATE DATE DEFAULT NULL,
            POSTED_DATE DATETIME DEFAULT NULL,
            INVOICE_NUMBER VARCHAR(50) DEFAULT NULL,
            DEBIT_CREDIT_INDICATOR VARCHAR(1) DEFAULT NULL,
            LOCAL_CURRENCY VARCHAR(10) DEFAULT NULL,
            TOTAL_AMOUNT_LC DECIMAL(18,2) DEFAULT NULL,
            INVOICE_CURRENCY VARCHAR(10) DEFAULT NULL,
            INVOICE_AMOUNT DECIMAL(18,2) DEFAULT NULL,
            CLEARING_DOCUMENT_NUMBER VARCHAR(50) DEFAULT NULL,
            CLEARING_DATE DATE DEFAULT NULL,
            PAYMENT_DATE DATE DEFAULT NULL,
            HEADER_TEXT VARCHAR(500) DEFAULT NULL,
            REVERSE_DOCUMENT_NUMBER VARCHAR(50) DEFAULT NULL,
            YEAR VARCHAR(10) DEFAULT NULL,
            REF_TRANSACTION VARCHAR(50) DEFAULT NULL,
            REFERENCE_KEY VARCHAR(50) DEFAULT NULL,
            DOCUMENT_HEADER_TEXT VARCHAR(500) DEFAULT NULL,
            ENTERED_DATE DATE DEFAULT NULL,
            EXCHANGE_RATE DECIMAL(18,6) DEFAULT NULL,
            ENTERED_BY VARCHAR(50) DEFAULT NULL,
            INVOICE_RECEIPT_DATE DATE DEFAULT NULL,
            PARTNER_BANK_TYPE VARCHAR(20) DEFAULT NULL,
            PAYMENT_BLOCK VARCHAR(10) DEFAULT NULL,
            PAYMENT_METHOD VARCHAR(50) DEFAULT NULL,
            PAYMENT_TERMS VARCHAR(50) DEFAULT NULL,
            BASELINE_DATE DATE DEFAULT NULL,
            DUE_DATE DATE DEFAULT NULL,
            PAYER VARCHAR(255) DEFAULT NULL,
            REASON_CODE VARCHAR(20) DEFAULT NULL,
            TRANSACTION_CODE VARCHAR(20) DEFAULT NULL,
            QUARTER_LABEL VARCHAR(20) DEFAULT NULL,
            PAYMENT_TERMS_DESCRIPTION VARCHAR(255) DEFAULT NULL,
            PAYMENT_METHOD_DESCRIPTION VARCHAR(255) DEFAULT NULL,
            REASON_CODE_DESCRIPTION VARCHAR(255) DEFAULT NULL,
            DELETION_INDICATOR_EKKO VARCHAR(1) DEFAULT NULL,
            CREATED_ON DATE DEFAULT NULL,
            SUPPLIER_ID_PO VARCHAR(45) DEFAULT NULL,
            PAYMENT_TERMS_PO VARCHAR(50) DEFAULT NULL,
            CURRENCY VARCHAR(10) DEFAULT NULL,
            EXCHANGE_RATE_PO DECIMAL(18,6) DEFAULT NULL,
            PURCHASING_DOCUMENT_DATE DATE DEFAULT NULL,
            SUPPLYING_VENDOR VARCHAR(255) DEFAULT NULL,
            INVOICING_PARTY VARCHAR(255) DEFAULT NULL,
            DOWN_PAYMENT_INDICATOR VARCHAR(1) DEFAULT NULL,
            VAT_REGISTRATION_NUMBER VARCHAR(50) DEFAULT NULL,
            VAT_REG_NO VARCHAR(50) DEFAULT NULL,
            ALTERNAT_PAYEE VARCHAR(255) DEFAULT NULL,
            VIM_DP_DOCUMENT_ID VARCHAR(50) DEFAULT NULL,
            DOW_GSTIN VARCHAR(50) DEFAULT NULL,
            VIM_DP_DOCUMENT_TYPE VARCHAR(50) DEFAULT NULL,
            VIM_DOCUMENT_STATUS VARCHAR(20) DEFAULT NULL,
            VIM_SPECIAL_STATUS VARCHAR(20) DEFAULT NULL,
            VIM_DP_EXPENSE_TYPE VARCHAR(50) DEFAULT NULL,
            VIM_DP_TRANSACTION_EVENT VARCHAR(50) DEFAULT NULL,
            VIM_OBJECT_TYPE VARCHAR(50) DEFAULT NULL,
            VIM_OBJECT_KEY VARCHAR(100) DEFAULT NULL,
            VIM_COMMENTS TEXT DEFAULT NULL,
            CHANNEL_ID VARCHAR(50) DEFAULT NULL,
            VIM_DOC_TYPE_DESC VARCHAR(255) DEFAULT NULL,
            VIM_DOC_STATUS_DESC VARCHAR(255) DEFAULT NULL,
            TRANSACTION_COUNT INT DEFAULT NULL,
            BILL_TO_LEGAL_ENTITY_NAME VARCHAR(255) DEFAULT NULL,
            BILL_TO_LEGAL_ENTITY_ADDRESS VARCHAR(500) DEFAULT NULL,
            TAX_CODE VARCHAR(20) DEFAULT NULL,
            TAX_AMOUNT DECIMAL(18,2) DEFAULT NULL,
            AUDIT_REASON VARCHAR(255) DEFAULT NULL,
            CREATED_DATE DATETIME DEFAULT CURRENT_TIMESTAMP,
            MODIFIED_DATE DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_account_doc_id (ACCOUNT_DOC_ID),
            INDEX idx_entry_id (ENTRY_ID)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(create_sql))
        log_message(f"Table {table_name} created successfully.")
        return True
    except Exception as e:
        log_message(f"Error creating table {table_name}: {e}",error_logger=True)
        return False


def create_zblock_transaction_table(table_name: str):
    """
    Create z_block_transaction_flat table with specified name."""
    engine = get_engine()

    table_name = str(table_name).lower().replace("-", "_").strip()
    log_message(f"Creating table: {table_name}")
    create_sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                        TRANSACTION_ID INT DEFAULT NULL,
                        ACCOUNT_DOC_ID INT DEFAULT NULL,
                        CLIENT VARCHAR(10) DEFAULT NULL,
                        COMPANY_CODE VARCHAR(45) DEFAULT NULL,
                        REGION VARCHAR(50) DEFAULT NULL,
                        DOCUMENT_NUMBER VARCHAR(50) DEFAULT NULL,
                        FISCAL_YEAR VARCHAR(10) DEFAULT NULL,
                        LINE_ITEM_ID VARCHAR(50) DEFAULT NULL,
                        DEBIT_CREDIT_INDICATOR VARCHAR(1) DEFAULT NULL,
                        GL_ACCOUNT_NUMBER VARCHAR(50) DEFAULT NULL,
                        LINEITEM_AMOUNT_IN_LOCAL_CURRENCY DECIMAL(18,2) DEFAULT NULL,
                        LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY DECIMAL(18,2) DEFAULT NULL,
                        PURCHASE_ORDER_NUMBER VARCHAR(50) DEFAULT NULL,
                        PO_ITEM_NUMBER VARCHAR(10) DEFAULT NULL,
                        TAX_CODE VARCHAR(20) DEFAULT NULL,
                        TAX_AMOUNT DECIMAL(18,2) DEFAULT NULL,
                        WITHHOLD_TAX_ITEM VARCHAR(10) DEFAULT NULL,
                        WITHHOLD_TAX_TYPE VARCHAR(10) DEFAULT NULL,
                        WITHHOLD_TAX_CODE VARCHAR(20) DEFAULT NULL,
                        WITHHOLD_TAX_BASE_LC DECIMAL(18,2) DEFAULT NULL,
                        WITHHOLD_TAX_BASE_FC DECIMAL(18,2) DEFAULT NULL,
                        PO_QUANTITY DECIMAL(18,3) DEFAULT NULL,
                        ORDER_UNIT VARCHAR(10) DEFAULT NULL,
                        NET_PRICE DECIMAL(18,2) DEFAULT NULL,
                        GROSS_VALUE DECIMAL(18,2) DEFAULT NULL,
                        GOODS_RECEIPT VARCHAR(50) DEFAULT NULL,
                        ITEM_CATEGORY VARCHAR(50) DEFAULT NULL,
                        EVALUATED_RECEIPT_SETTLEMENT VARCHAR(1) DEFAULT NULL,
                        ORIGIN VARCHAR(50) DEFAULT NULL,
                        ORIGIN_REGION VARCHAR(50) DEFAULT NULL,
                        DESTINATION_COUNTRY VARCHAR(50) DEFAULT NULL,
                        DESTINATION_REGION VARCHAR(50) DEFAULT NULL,
                        CREATED_AT DATETIME DEFAULT CURRENT_TIMESTAMP,
                        MODIFIED_AT DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY uk_transaction_id (TRANSACTION_ID),
                        INDEX idx_account_doc_id (ACCOUNT_DOC_ID),
                        INDEX idx_document_number (DOCUMENT_NUMBER)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"""
    
    try:
        with engine.begin() as conn:
            conn.execute(text(create_sql))
        log_message(f"Table {table_name} created successfully.")
        return True
    except Exception as e:
        log_message(f"Error creating table {table_name}: {e}",error_logger=True)
        return False

def create_flat_tables_based_on_quarters(quarters: List[str]):    
    """
    Create flat tables for each specified quarter.
    
    Args:
        quarters: List of quarter labels (e.g., ['q1_2025', 'q2_2025'])
    """
    log_message(f"Creating header and line item level tables for quarters: {quarters}")
    for quarter in quarters:
        transaction_flat_table_name = f"z_block_transaction_flat_{quarter}"
        account_document_flat_table_name = f"z_block_account_document_flat_{quarter}"
        trans_status =create_zblock_transaction_table(transaction_flat_table_name)
        if not trans_status:
            log_message(f"Failed to create transaction table for quarter {quarter}")
            raise Exception(f"Failed to create transaction table for quarter {quarter}")
        
        acc_doc_status  = create_zblock_account_document_table(account_document_flat_table_name)
        if not acc_doc_status:
            log_message(f"Failed to create account document table for quarter {quarter}")
            raise Exception(f"Failed to create account document table for quarter {quarter}")
        


def insert_rows_in_zblock_transaction_table(sap_df, batch_size=5000)-> bool:
    """
    Insert rows into z_block_transaction_flat tables based on quarter using pd.to_sql().
    
    Args:
        sap_df: DataFrame containing SAP transaction data with QUARTER_LABEL column
        batch_size: Number of records to insert per batch (default 5000)
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        import pandas as pd
        import numpy as np
        
        engine = get_engine()
        
        # Validate QUARTER_LABEL column exists
        if 'QUARTER_LABEL' not in sap_df.columns:
            log_message("Error: QUARTER_LABEL column not found in DataFrame", error_logger=True)
            return False
        
        # Validate DataFrame is not empty
        if sap_df.empty:
            log_message("Warning: Empty DataFrame provided, nothing to insert", error_logger=True)
            return True
        
        # Group by QUARTER_LABEL
        quarter_groups = sap_df.groupby('QUARTER_LABEL')
        log_message(f"Found {len(quarter_groups)} quarters in data: {list(quarter_groups.groups.keys())}")
        
        # Column mapping: DataFrame column name -> z_block_transaction_flat table column name
        column_mapping = {
            'TRANSACTION_ID': 'TRANSACTION_ID',
            'ACCOUNT_DOC_ID': 'ACCOUNT_DOC_ID',
            'CLIENT': 'CLIENT',
            'COMPANY_CODE': 'COMPANY_CODE',
            'REGION_BSEG': 'REGION',
            'DOCUMENT_NUMBER': 'DOCUMENT_NUMBER',
            'FISCAL_YEAR': 'FISCAL_YEAR',
            'LINE_ITEM_ID': 'LINE_ITEM_ID',
            'DEBIT_CREDIT_INDICATOR': 'DEBIT_CREDIT_INDICATOR',
            'GL_ACCOUNT_NUMBER': 'GL_ACCOUNT_NUMBER',
            'LINEITEM_AMOUNT_IN_LOCAL_CURRENCY': 'LINEITEM_AMOUNT_IN_LOCAL_CURRENCY',
            'LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY': 'LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY',
            'PURCHASE_ORDER_NUMBER': 'PURCHASE_ORDER_NUMBER',
            'PO_ITEM_NUMBER': 'PO_ITEM_NUMBER',
            'TAX_CODE': 'TAX_CODE',
            'TAX_AMOUNT': 'TAX_AMOUNT',
            # 'ITEM_TEXT': 'ITEM_TEXT',  # Excluded as not in table schema
            'WITHHOLD_TAX_ITEM': 'WITHHOLD_TAX_ITEM',
            'WITHHOLD_TAX_TYPE': 'WITHHOLD_TAX_TYPE',
            'WITHHOLD_TAX_CODE': 'WITHHOLD_TAX_CODE',
            'WITHHOLD_TAX_BASE_LC': 'WITHHOLD_TAX_BASE_LC',
            'WITHHOLD_TAX_BASE_FC': 'WITHHOLD_TAX_BASE_FC',
            'PO_QUANTITY': 'PO_QUANTITY',
            'ORDER_UNIT': 'ORDER_UNIT',
            'NET_PRICE': 'NET_PRICE',
            'GROSS_VALUE': 'GROSS_VALUE',
            'GOODS_RECEIPT': 'GOODS_RECEIPT',
            'ITEM_CATEGORY': 'ITEM_CATEGORY',
            'EVALUATED_RECEIPT_SETTLEMENT': 'EVALUATED_RECEIPT_SETTLEMENT',
            'ORIGIN': 'ORIGIN',
            'ORIGIN_REGION': 'ORIGIN_REGION',
            'DESTINATION_COUNTRY': 'DESTINATION_COUNTRY',
            'DESTINATION_REGION': 'DESTINATION_REGION'
        }
        
        total_inserted = 0
        
        # Process each quarter's data
        for quarter_label, quarter_df in quarter_groups:
            log_message(f"Processing quarter: {quarter_label} with {len(quarter_df)} rows")
            
            # Create a copy to avoid modifying original
            df_to_insert = quarter_df.copy()
            
            # Select only columns that exist in both mapping and DataFrame
            columns_to_use = [src for src in column_mapping.keys() if src in df_to_insert.columns]
            
            if not columns_to_use:
                log_message(f"Warning: No matching columns found for quarter {quarter_label}", error_logger=True)
                continue
            
            log_message(f"Using {len(columns_to_use)} columns out of total columns in df {len(df_to_insert.columns)} for insertion for quarter {quarter_label}")
            null_counts_before = df_to_insert.isnull().sum()
            log_message(f"Null value counts before insertion for quarter {quarter_label}:\n{null_counts_before[null_counts_before > 0].to_dict()}")
            
            df_to_insert = df_to_insert[columns_to_use]
            
            # Clean data at DataFrame level
            df_to_insert = df_to_insert.replace({pd.NA: None, pd.NaT: None, '': None, np.nan: None})
            df_to_insert = df_to_insert.where(pd.notnull(df_to_insert), None)
            
            # Rename columns to match database column names
            df_to_insert = df_to_insert.rename(columns=column_mapping)
            
            log_message(f"Cleaned and renamed {len(columns_to_use)} columns for quarter {quarter_label}")
            
            # Prepare table name
            quarter_normalized = quarter_label.lower().replace("-", "_").strip()
            table_name = f"z_block_transaction_flat_{quarter_normalized}"
            
            # Insert/Update using pd.to_sql() with MySQL UPSERT
            df_to_insert.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False,
                chunksize=batch_size,
                method=mysql_upsert_method  # Uses INSERT ... ON DUPLICATE KEY UPDATE
            )
            
            total_inserted += len(df_to_insert)
            log_message(f"Inserted {len(df_to_insert)} rows into {table_name}")
        
        log_message(f"insert_rows_in_zblock_transaction_table completed successfully. Total inserted: {total_inserted} rows")
        return True
        
    except Exception as e:
        log_message(f"Error in insert_rows_in_zblock_transaction_table: {e}", error_logger=True)
        import traceback
        log_message(traceback.format_exc(), error_logger=True)
        return False


def insert_rows_in_zblock_acc_doc_table(sap_df, batch_size=5000)-> bool:
    """
    Insert rows into z_block_account_document_flat tables based on quarter using pd.to_sql().
    Ensures one row per unique accounting document by deduplicating on DOCUMENT_NUMBER.
    
    Args:
        sap_df: DataFrame containing SAP account document data with QUARTER_LABEL column
        batch_size: Number of records to insert per batch (default 5000)
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        import pandas as pd
        import numpy as np
        
        engine = get_engine()
        
        # Validate QUARTER_LABEL column exists
        if 'QUARTER_LABEL' not in sap_df.columns:
            log_message("Error: QUARTER_LABEL column not found in DataFrame", error_logger=True)
            return False
        
        # Validate DataFrame is not empty
        if sap_df.empty:
            log_message("Warning: Empty DataFrame provided, nothing to insert", error_logger=True)
            return True
        
        # Group by QUARTER_LABEL
        quarter_groups = sap_df.groupby('QUARTER_LABEL')
        log_message(f"Found {len(quarter_groups)} quarters in data: {list(quarter_groups.groups.keys())}")
        
        # Column mapping: DataFrame column name -> z_block_account_document_flat table column name
        column_mapping = {
            'ACCOUNT_DOC_ID': 'ACCOUNT_DOC_ID',
            'CLIENT': 'CLIENT',
            'COMPANY_CODE': 'COMPANY_CODE',
            'REGION_BKPF': 'REGION',
            'DOCUMENT_NUMBER': 'ENTRY_ID',
            'FISCAL_YEAR': 'FISCAL_YEAR',
            'SUPPLIER_ID': 'VENDORCODE',
            'VENDOR_NAME': 'VENDOR_NAME',
            'VENDOR_ADDRESS': 'VENDOR_ADDRESS',
            'DOCUMENT_TYPE': 'DOCUMENT_TYPE',
            'INVOICE_DATE': 'INVOICE_DATE',
            'POSTED_DATE': 'POSTED_DATE',
            'INVOICE_NUMBER': 'INVOICE_NUMBER',
            'DEBIT_CREDIT_INDICATOR': 'DEBIT_CREDIT_INDICATOR',
            'LOCAL_CURRENCY': 'LOCAL_CURRENCY',
            'TOTAL_AMOUNT_LC': 'TOTAL_AMOUNT_LC',
            'DOCUMENT_CURRENCY': 'INVOICE_CURRENCY',
            'TOTAL_AMOUNT': 'INVOICE_AMOUNT',
            'CLEARING_DOCUMENT_NUMBER': 'CLEARING_DOCUMENT_NUMBER',
            'CLEARING_DATE': 'CLEARING_DATE',
            'PAYMENT_DATE': 'PAYMENT_DATE',
            'HEADER_TEXT': 'HEADER_TEXT',
            'REVERSE_DOCUMENT_NUMBER': 'REVERSE_DOCUMENT_NUMBER',
            'YEAR': 'YEAR',
            'REF_TRANSACTION': 'REF_TRANSACTION',
            'REFERENCE_KEY': 'REFERENCE_KEY',
            'DOCUMENT_HEADER_TEXT': 'DOCUMENT_HEADER_TEXT',
            'ENTERED_DATE': 'ENTERED_DATE',
            'EXCHANGE_RATE': 'EXCHANGE_RATE',
            'ENTERED_BY': 'ENTERED_BY',
            'INVOICE_RECEIPT_DATE': 'INVOICE_RECEIPT_DATE',
            'PARTNER_BANK_TYPE': 'PARTNER_BANK_TYPE',
            'PAYMENT_BLOCK': 'PAYMENT_BLOCK',
            'PAYMENT_METHOD': 'PAYMENT_METHOD',
            'PAYMENT_TERMS_Invoice': 'PAYMENT_TERMS',
            'BASELINE_DATE': 'BASELINE_DATE',
            'DUE_DATE': 'DUE_DATE',
            'PAYER': 'PAYER',
            'REASON_CODE': 'REASON_CODE',
            'TRANSACTION_CODE': 'TRANSACTION_CODE',
            'PAYMENT_TERMS_DESCRIPTION': 'PAYMENT_TERMS_DESCRIPTION',
            'PAYMENT_METHOD_DESCRIPTION': 'PAYMENT_METHOD_DESCRIPTION',
            'REASON_CODE_DESCRIPTION': 'REASON_CODE_DESCRIPTION',
            'DELETION_INDICATOR_EKKO': 'DELETION_INDICATOR_EKKO',
            'CREATED_ON': 'CREATED_ON',
            'SUPPLIER_ID_PO': 'SUPPLIER_ID_PO',
            'PAYMENT_TERMS_PO': 'PAYMENT_TERMS_PO',
            'PO_CURRENCY': 'CURRENCY',
            'EXCHANGE_RATE_PO': 'EXCHANGE_RATE_PO',
            'PURCHASING_DOCUMENT_DATE': 'PURCHASING_DOCUMENT_DATE',
            'SUPPLYING_VENDOR': 'SUPPLYING_VENDOR',
            'INVOICING_PARTY': 'INVOICING_PARTY',
            'DOWN_PAYMENT_INDICATOR': 'DOWN_PAYMENT_INDICATOR',
            'VAT_REGISTRATION_NUMBER': 'VAT_REGISTRATION_NUMBER',
            'VENDOR_VAT_REG_NO': 'VAT_REG_NO',
            'ALTERNATE_PAYEE': 'ALTERNAT_PAYEE',
            'VIM_DP_DOCUMENT_ID': 'VIM_DP_DOCUMENT_ID',
            'DOW_GSTIN': 'DOW_GSTIN',
            'VIM_DP_DOCUMENT_TYPE': 'VIM_DP_DOCUMENT_TYPE',
            'VIM_DOCUMENT_STATUS': 'VIM_DOCUMENT_STATUS',
            'VIM_SPECIAL_STATUS': 'VIM_SPECIAL_STATUS',
            'VIM_DP_EXPENSE_TYPE': 'VIM_DP_EXPENSE_TYPE',
            'VIM_DP_TRANSACTION_EVENT': 'VIM_DP_TRANSACTION_EVENT',
            'VIM_OBJECT_TYPE': 'VIM_OBJECT_TYPE',
            'VIM_OBJECT_KEY': 'VIM_OBJECT_KEY',
            'CHANNEL_ID': 'CHANNEL_ID',
            'VIM_DOC_TYPE_DESC': 'VIM_DOC_TYPE_DESC',
            'VIM_DOC_STATUS_DESC': 'VIM_DOC_STATUS_DESC',
            'VIM_COMMENTS': 'VIM_COMMENTS',
            'TRANSACTION_COUNT': 'TRANSACTION_COUNT',
            'LE_NAME': 'BILL_TO_LEGAL_ENTITY_NAME',
            'LE_ADDRESS': 'BILL_TO_LEGAL_ENTITY_ADDRESS',
            'TAX_CODE': 'TAX_CODE',
            'TAX_AMOUNT': 'TAX_AMOUNT',
            'AUDIT_REASON': 'AUDIT_REASON'
        }
        
        total_inserted = 0
        
        # Process each quarter's data
        for quarter_label, quarter_df in quarter_groups:
            log_message(f"Processing quarter: {quarter_label} with {len(quarter_df)} rows")
            
            # Create a copy to avoid modifying original
            df_to_insert = quarter_df.copy()
            
            # CRITICAL: Deduplicate by unique accounting document key (CLIENT, COMPANY_CODE, DOCUMENT_NUMBER, FISCAL_YEAR)
            dedup_columns = ['CLIENT', 'COMPANY_CODE', 'DOCUMENT_NUMBER', 'FISCAL_YEAR']
            existing_dedup_cols = [col for col in dedup_columns if col in df_to_insert.columns]
            
            if existing_dedup_cols:
                original_count = len(df_to_insert)
                df_to_insert = df_to_insert.drop_duplicates(subset=existing_dedup_cols, keep='first')
                deduplicated_count = len(df_to_insert)
                log_message(f"Deduplicated from {original_count} to {deduplicated_count} rows using {existing_dedup_cols}")
            else:
                log_message("Warning: Deduplication columns not found, cannot deduplicate", error_logger=True)
            
            # Select only columns that exist in both mapping and DataFrame
            columns_to_use = [src for src in column_mapping.keys() if src in df_to_insert.columns]
            
            if not columns_to_use:
                log_message(f"Warning: No matching columns found for quarter {quarter_label}", error_logger=True)
                continue
            # null value count >0 of dataframe before insertion
            null_counts_before = df_to_insert.isnull().sum()

            log_message(f"Null value counts before insertion for quarter {quarter_label}:\n{null_counts_before[null_counts_before > 0].to_dict()}")
            log_message(f"Using {len(columns_to_use)} columns out of total columns in df {len(df_to_insert.columns)} for insertion for quarter {quarter_label}")
            df_to_insert = df_to_insert[columns_to_use]
            
            # Clean data at DataFrame level
            df_to_insert = df_to_insert.replace({pd.NA: None, pd.NaT: None, '': None, np.nan: None})
            df_to_insert = df_to_insert.where(pd.notnull(df_to_insert), None)
            
            # Rename columns to match database column names
            df_to_insert = df_to_insert.rename(columns=column_mapping)
            
            log_message(f"Cleaned and renamed {len(columns_to_use)} columns for quarter {quarter_label}")
            
            # Prepare table name
            quarter_normalized = quarter_label.lower().replace("-", "_").strip()
            table_name = f"z_block_account_document_flat_{quarter_normalized}"
            
            # Insert/Update using pd.to_sql() with MySQL UPSERT
            df_to_insert.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False,
                chunksize=batch_size,
                method=mysql_upsert_method  # Uses INSERT ... ON DUPLICATE KEY UPDATE
            )
            
            total_inserted += len(df_to_insert)
            log_message(f"Inserted {len(df_to_insert)} rows into {table_name}")
        
        log_message(f"insert_rows_in_zblock_acc_doc_table completed successfully. Total inserted: {total_inserted} rows")
        return True
        
    except Exception as e:
        log_message(f"Error in insert_rows_in_zblock_acc_doc_table: {e}", error_logger=True)
        import traceback
        log_message(traceback.format_exc(), error_logger=True)
        return False
























































































# def insert_quarter_rows(sap_rows:List[SAPRow]):
#     """
#     Insert multiple SAPRow objects into quarter-specific tables.
#     Groups rows by quarter_label so each table is created once,
#     then inserts all rows for that quarter in batch.
    
#     Args:
#         sap_rows: List of SAPRow objects from Schemas/sap_row.py
#     """
#     engine = get_engine()

#     # Group rows by quarter
#     quarter_groups = {}
#     for row in sap_rows:
#         quarter = row.quarter_label.lower().replace("-", "_").strip()
#         quarter_groups.setdefault(quarter, []).append(row)

#     # Use a transaction block so commit/rollback is handled automatically
#     with engine.begin() as conn:
#         for quarter, rows in quarter_groups.items():
#             table_name = f"z_block_account_document_flat_{quarter}"

#             # Create table once per quarter with updated structure
#             create_sql = f"""
#             CREATE TABLE IF NOT EXISTS {table_name} (
#               ACCOUNT_DOC_ID VARCHAR(50) DEFAULT NULL,
#               CLIENT VARCHAR(10) DEFAULT NULL,
#               COMPANY_CODE VARCHAR(45) DEFAULT NULL,
#               REGION VARCHAR(50) DEFAULT NULL,
#               ENTRY_ID VARCHAR(50) DEFAULT NULL,
#               FISCAL_YEAR VARCHAR(10) DEFAULT NULL,
#               TAX_CODE VARCHAR(20) DEFAULT NULL,
#               VENDORCODE VARCHAR(45) DEFAULT NULL,
#               VENDOR_NAME VARCHAR(255) DEFAULT NULL,
#               VENDOR_ADDRESS VARCHAR(500) DEFAULT NULL,
#               DOCUMENT_TYPE VARCHAR(10) DEFAULT NULL,
#               INVOICE_DATE DATE DEFAULT NULL,
#               POSTED_DATE DATETIME DEFAULT NULL,
#               INVOICE_NUMBER VARCHAR(50) DEFAULT NULL,
#               DEBIT_CREDIT_INDICATOR VARCHAR(1) DEFAULT NULL,
#               LOCAL_CURRENCY VARCHAR(10) DEFAULT NULL,
#               TOTAL_AMOUNT_LC DECIMAL(18,2) DEFAULT NULL,
#               INVOICE_CURRENCY VARCHAR(10) DEFAULT NULL,
#               INVOICE_AMOUNT DECIMAL(18,2) DEFAULT NULL,
#               CLEARING_DOCUMENT_NUMBER VARCHAR(50) DEFAULT NULL,
#               CLEARING_DATE DATE DEFAULT NULL,
#               REVERSE_DOCUMENT_NUMBER VARCHAR(50) DEFAULT NULL,
#               REF_TRANSACTION VARCHAR(50) DEFAULT NULL,
#               REFERENCE_KEY VARCHAR(50) DEFAULT NULL,
#               DOCUMENT_HEADER_TEXT VARCHAR(500) DEFAULT NULL,
#               ENTERED_DATE DATE DEFAULT NULL,
#               EXCHANGE_RATE DECIMAL(18,6) DEFAULT NULL,
#               ENTERED_BY VARCHAR(50) DEFAULT NULL,
#               INVOICE_RECEIPT_DATE DATE DEFAULT NULL,
#               PARTNER_BANK_TYPE VARCHAR(20) DEFAULT NULL,
#               PAYMENT_METHOD VARCHAR(50) DEFAULT NULL,
#               PAYMENT_TERMS VARCHAR(50) DEFAULT NULL,
#               DUE_DATE DATE DEFAULT NULL,
#               PAYER VARCHAR(255) DEFAULT NULL,
#               REASON_CODE VARCHAR(20) DEFAULT NULL,
#               QUARTER_LABEL VARCHAR(20) DEFAULT NULL,
#               WITHHOLD_TAX_ITEM VARCHAR(10) DEFAULT NULL,
#               WITHHOLD_TAX_TYPE VARCHAR(10) DEFAULT NULL,
#               WITHHOLD_TAX_CODE VARCHAR(20) DEFAULT NULL,
#               WITHHOLD_TAX_BASE_LC DECIMAL(18,2) DEFAULT NULL,
#               WITHHOLD_TAX_BASE_FC DECIMAL(18,2) DEFAULT NULL,
#               DOCUMENT_TYPE_DESCRIPTION VARCHAR(255) DEFAULT NULL,
#               PAYMENT_TERMS_DESCRIPTION VARCHAR(255) DEFAULT NULL,
#               PAYMENT_METHOD_DESCRIPTION VARCHAR(255) DEFAULT NULL,
#               REASON_CODE_DESCRIPTION VARCHAR(255) DEFAULT NULL,
#               PAYEE VARCHAR(255) DEFAULT NULL,
#               DOW_GSTIN VARCHAR(50) DEFAULT NULL,
#               VIM_DP_DOCUMENT_TYPE VARCHAR(50) DEFAULT NULL,
#               VIM_DP_EXPENSE_TYPE VARCHAR(50) DEFAULT NULL,
#               CHANNEL_ID VARCHAR(50) DEFAULT NULL,
#               VIM_DOC_TYPE_DESC VARCHAR(255) DEFAULT NULL,
#               TRANSACTION_CODE VARCHAR(20) DEFAULT NULL,
#               TRANSACTION_ID VARCHAR(100) DEFAULT NULL,
#               TRANSACTION_COUNT INT DEFAULT NULL,
#               CREATED_DATE DATETIME DEFAULT CURRENT_TIMESTAMP,
#               MODIFIED_DATE DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
#             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
#             """
#             conn.execute(text(create_sql))

#             # Prepare insert with fields mapped from SAPRow attributes
#             insert_sql = f"""
#             INSERT INTO {table_name} (
#               ACCOUNT_DOC_ID, TRANSACTION_ID, CLIENT, COMPANY_CODE, REGION, ENTRY_ID, FISCAL_YEAR,
#               TAX_CODE, VENDORCODE, DOCUMENT_TYPE, INVOICE_DATE, POSTED_DATE,
#               INVOICE_NUMBER, DEBIT_CREDIT_INDICATOR, LOCAL_CURRENCY, TOTAL_AMOUNT_LC,
#               INVOICE_CURRENCY, INVOICE_AMOUNT, CLEARING_DOCUMENT_NUMBER, CLEARING_DATE,
#               REVERSE_DOCUMENT_NUMBER, REF_TRANSACTION, REFERENCE_KEY,
#               DOCUMENT_HEADER_TEXT, ENTERED_DATE, EXCHANGE_RATE, ENTERED_BY,
#               INVOICE_RECEIPT_DATE, PARTNER_BANK_TYPE, PAYMENT_METHOD, PAYMENT_TERMS,
#               DUE_DATE, PAYER, REASON_CODE, QUARTER_LABEL, WITHHOLD_TAX_ITEM,
#               WITHHOLD_TAX_TYPE, WITHHOLD_TAX_CODE, WITHHOLD_TAX_BASE_LC, WITHHOLD_TAX_BASE_FC,
#               DOCUMENT_TYPE_DESCRIPTION, PAYMENT_TERMS_DESCRIPTION, PAYMENT_METHOD_DESCRIPTION,
#               REASON_CODE_DESCRIPTION, PAYEE, DOW_GSTIN, VIM_DP_DOCUMENT_TYPE,
#               VIM_DP_EXPENSE_TYPE, CHANNEL_ID, VIM_DOC_TYPE_DESC, TRANSACTION_CODE,  TRANSACTION_COUNT, VENDOR_NAME
#             ) VALUES (
#               :account_doc_id, :transaction_id, :tenant_code, :company_code, :region, :account_document_number, :fiscal_year,
#               :vat_tax_id, :vendor_code, :doc_type, :invoice_date, :posted_date,
#               :invoice_number, :debit_credit_indicator, :local_currency, :total_amount_lc,
#               :invoice_currency, :invoice_amount, :clearing_document_number, :clearing_date,
#               :reverse_document_number, :ref_transaction, :reference_key,
#               :text_field, :entered_date, :exchange_rate, :entered_by,
#               :invoice_receipt_date, :partner_bank_type, :payment_method, :payment_terms,
#               :due_date, :payer, :payment_term_reason_code, :quarter_label, :wht_item,
#               :wht_type, :wht_code, :wht_base_lc, :wht_base_fc,
#               :doc_type_description, :payment_term_description, :payment_method_description,
#               :reason_code_description, :payee, :dow_gst_in_number, :dp_doc_type,
#               :expense_type, :channel_id, :dp_doc_type_description, :transaction_code,  :transaction_count ,:vendor_name
#             )
#             """

#             # Map SAPRow attributes to database columns
#             sanitized_rows = []
#             for sap_row in rows:
#                 sap_row: SAPRow
#                 row_data = {
#                     'transaction_count': sap_row.transaction_count,
#                     'account_doc_id': sap_row.account_document_id,
#                     'tenant_code': sap_row.tenant_code,
#                     'company_code': sap_row.company_code,
#                     'region': sap_row.region,
#                     'account_document_number': sap_row.account_document_number,
#                     'fiscal_year': sap_row.fiscal_year,
#                     'vat_tax_id': sap_row.vat_tax_id,
#                     'vendor_code': sap_row.vendor_code,
#                     'doc_type': sap_row.doc_type,
#                     'invoice_date': sap_row.invoice_date,
#                     'posted_date': sap_row.posted_date,
#                     'invoice_number': sap_row.invoice_number,
#                     'debit_credit_indicator': sap_row.debit_credit_indicator,
#                     'local_currency': sap_row.local_currency,
#                     'total_amount_lc': sap_row.total_amount_lc,
#                     'invoice_currency': sap_row.invoice_currency,
#                     'invoice_amount': sap_row.invoice_amount,
#                     'clearing_document_number': sap_row.clearing_document_number,
#                     'clearing_date': sap_row.clearing_date,
#                     'reverse_document_number': sap_row.reverse_document_number,
#                     'ref_transaction': sap_row.ref_transaction,
#                     'reference_key': sap_row.reference_key,
#                     'text_field': sap_row.text_field,
#                     'entered_date': sap_row.entered_date,
#                     'exchange_rate': sap_row.exchange_rate,
#                     'entered_by': sap_row.entered_by,
#                     'invoice_receipt_date': sap_row.invoice_receipt_date,
#                     'partner_bank_type': sap_row.partner_bank_type,
#                     'payment_method': sap_row.payment_method,
#                     'payment_terms': sap_row.payment_terms,
#                     'due_date': sap_row.due_date,
#                     'payer': sap_row.payer,
#                     'payment_term_reason_code': sap_row.payment_term_reason_code,
#                     'quarter_label': sap_row.quarter_label,
#                     'wht_item': sap_row.wht_item,
#                     'wht_type': sap_row.wht_type,
#                     'wht_code': sap_row.wht_code,
#                     'wht_base_lc': sap_row.wht_base_lc,
#                     'wht_base_fc': sap_row.wht_base_fc,
#                     'doc_type_description': sap_row.doc_type_description,
#                     'payment_term_description': sap_row.payment_term_description,
#                     'payment_method_description': sap_row.payment_method_description,
#                     'reason_code_description': sap_row.reason_code_description,
#                     'payee': sap_row.payee,
#                     'dow_gst_in_number': sap_row.dow_gst_in_number,
#                     'dp_doc_type': sap_row.dp_doc_type,
#                     'expense_type': sap_row.expense_type,
#                     'channel_id': sap_row.channel_id,
#                     'dp_doc_type_description': sap_row.dp_doc_type_description,
#                     'transaction_code': sap_row.transaction_code,
#                     'transaction_id': sap_row.transaction_id,
#                     'vendor_name': sap_row.vendor_name
#                 }
#                 sanitized_rows.append(sanitize_parameters(row_data))

#             # Batch insert all rows for this quarter
#             conn.execute(text(insert_sql), sanitized_rows)


# def insert_transaction_rows(group_df, account_doc_id, quarter_label):
#     """
#     Insert transaction-level data into quarter-specific z_block_transaction_flat tables.
#     Each row in the group_df represents a transaction that belongs to an account document.

#     Args:
#         group_df: DataFrame containing transaction rows for a single account document
#         account_doc_id: The account document ID this transaction belongs to
#         quarter_label: Quarter label (e.g., 'Q1-2025')
#     """
#     engine = get_engine()

#     quarter = quarter_label.lower().replace("-", "_").strip()
#     table_name = f"z_block_transaction_flat_{quarter}"

#     # Use a transaction block
#     with engine.begin() as conn:
#         # Create table if not exists
#         create_sql = f"""
#         CREATE TABLE IF NOT EXISTS {table_name} (
#           TRANSACTION_ID INT DEFAULT NULL,
#           ACCOUNT_DOC_ID INT DEFAULT NULL,
#           CLIENT VARCHAR(10) DEFAULT NULL,
#           COMPANY_CODE VARCHAR(45) DEFAULT NULL,
#           REGION VARCHAR(50) DEFAULT NULL,
#           ENTRY_ID VARCHAR(50) DEFAULT NULL,
#           FISCAL_YEAR VARCHAR(10) DEFAULT NULL,
#           ITEM VARCHAR(10) DEFAULT NULL,
#           LINE_ITEM_ID VARCHAR(50) DEFAULT NULL,
#           DEBIT_CREDIT_INDICATOR VARCHAR(1) DEFAULT NULL,
#           GL_ACCOUNT_NUMBER VARCHAR(50) DEFAULT NULL,
#           LINEITEM_AMOUNT_IN_LOCAL_CURRENCY DECIMAL(18,2) DEFAULT NULL,
#           LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY DECIMAL(18,2) DEFAULT NULL,
#           PURCHASE_ORDER_NUMBER VARCHAR(50) DEFAULT NULL,
#           PO_ITEM_NUMBER VARCHAR(10) DEFAULT NULL,
#           ITEM_TEXT VARCHAR(500) DEFAULT NULL,
#           VENDORCODE VARCHAR(45) DEFAULT NULL,
#           DOCUMENT_TYPE VARCHAR(10) DEFAULT NULL,
#           INVOICE_DATE DATE DEFAULT NULL,
#           POSTED_DATE DATETIME DEFAULT NULL,
#           INVOICE_NUMBER VARCHAR(50) DEFAULT NULL,
#           PAYMENT_DATE DATE DEFAULT NULL,
#           DUE_DATE DATE DEFAULT NULL,
#           PAYER VARCHAR(255) DEFAULT NULL,
#           REASON_CODE VARCHAR(20) DEFAULT NULL,
#           QUARTER_LABEL VARCHAR(20) DEFAULT NULL,
#           WITHHOLD_TAX_ITEM VARCHAR(10) DEFAULT NULL,
#           WITHHOLD_TAX_TYPE VARCHAR(10) DEFAULT NULL,
#           WITHHOLD_TAX_CODE VARCHAR(20) DEFAULT NULL,
#           WITHHOLD_TAX_BASE_LC DECIMAL(18,2) DEFAULT NULL,
#           WITHHOLD_TAX_BASE_FC DECIMAL(18,2) DEFAULT NULL,
#           DOCUMENT_TYPE_DESCRIPTION VARCHAR(255) DEFAULT NULL,
#           ITEM_CATEGORY VARCHAR(50) DEFAULT NULL,
#           PAYMENT_TERMS_PO VARCHAR(50) DEFAULT NULL,
#           PO_CURRENCY VARCHAR(10) DEFAULT NULL,
#           EXCHANGE_RATE_PO DECIMAL(18,6) DEFAULT NULL,
#           SUPPLYING_VENDOR VARCHAR(255) DEFAULT NULL,
#           INVOICING_PARTY VARCHAR(255) DEFAULT NULL,
#           DOWN_PAYMENT_INDICATOR VARCHAR(1) DEFAULT NULL,
#           VAT_REGISTRATION_NUMBER VARCHAR(50) DEFAULT NULL,
#           PAYEE VARCHAR(255) DEFAULT NULL,
#           DOW_GSTIN VARCHAR(50) DEFAULT NULL,
#           VIM_DP_DOCUMENT_TYPE VARCHAR(50) DEFAULT NULL,
#           VIM_DP_EXPENSE_TYPE VARCHAR(50) DEFAULT NULL,
#           CHANNEL_ID VARCHAR(50) DEFAULT NULL,
#           VIM_DOC_TYPE_DESC VARCHAR(255) DEFAULT NULL,
#           PAYMENT_METHOD VARCHAR(50) DEFAULT NULL,
#           VENDOR_NAME VARCHAR(255) DEFAULT NULL,
#           VENDOR_ADDRESS VARCHAR(500) DEFAULT NULL,
#           BILL_TO_LEGAL_ENTITY_NAME VARCHAR(255) DEFAULT NULL,
#           BILL_TO_LEGAL_ENTITY_ADDRESS VARCHAR(500) DEFAULT NULL,
#           CREATED_AT DATETIME DEFAULT CURRENT_TIMESTAMP,
#           MODIFIED_AT DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
#           INDEX idx_account_doc_id (ACCOUNT_DOC_ID),
#           INDEX idx_accounting_doc (ENTRY_ID)
#         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
#         """
#         conn.execute(text(create_sql))

#         # Prepare insert statement
#         insert_sql = f"""
#         INSERT INTO {table_name} (
#           TRANSACTION_ID,ACCOUNT_DOC_ID, CLIENT, COMPANY_CODE, REGION, ENTRY_ID,
#           FISCAL_YEAR, ITEM, LINE_ITEM_ID, DEBIT_CREDIT_INDICATOR, GL_ACCOUNT_NUMBER,
#           LINEITEM_AMOUNT_IN_LOCAL_CURRENCY, LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY,
#           PURCHASE_ORDER_NUMBER, PO_ITEM_NUMBER, ITEM_TEXT, VENDORCODE,
#           DOCUMENT_TYPE, INVOICE_DATE, POSTED_DATE, INVOICE_NUMBER,
#           PAYMENT_DATE, DUE_DATE, PAYER, REASON_CODE, QUARTER_LABEL,
#           WITHHOLD_TAX_ITEM, WITHHOLD_TAX_TYPE, WITHHOLD_TAX_CODE,
#           WITHHOLD_TAX_BASE_LC, WITHHOLD_TAX_BASE_FC, DOCUMENT_TYPE_DESCRIPTION,
#           ITEM_CATEGORY, PAYMENT_TERMS_PO, PO_CURRENCY, EXCHANGE_RATE_PO,
#            SUPPLYING_VENDOR, INVOICING_PARTY,
#           DOWN_PAYMENT_INDICATOR, VAT_REGISTRATION_NUMBER, PAYEE, DOW_GSTIN,
#           VIM_DP_DOCUMENT_TYPE, VIM_DP_EXPENSE_TYPE, CHANNEL_ID, VIM_DOC_TYPE_DESC, PAYMENT_METHOD,
#             VENDOR_NAME, VENDOR_ADDRESS, BILL_TO_LEGAL_ENTITY_NAME, BILL_TO_LEGAL_ENTITY_ADDRESS
#         ) VALUES (
#           :transaction_id, :account_doc_id, :client, :company_code, :region, :accounting_document_number,
#           :fiscal_year, :item, :line_item_id, :debit_credit_indicator, :gl_account_number,
#           :lineitem_amount_in_local_currency, :lineitem_amount_in_document_currency,
#           :purchase_order_number, :po_item_number, :item_text, :supplier_id,
#           :document_type, :invoice_date, :posted_date, :invoice_number,
#           :payment_date, :due_date, :payer, :reason_code, :quarter_label,
#           :withhold_tax_item, :withhold_tax_type, :withhold_tax_code,
#           :withhold_tax_base_lc, :withhold_tax_base_fc, :document_type_description,
#           :item_category, :payment_terms_po, :po_currency, :exchange_rate_po,
#           :supplying_vendor, :invoicing_party,
#           :down_payment_indicator, :vat_registration_number, :payee, :dow_gstin,
#           :vim_dp_document_type, :vim_dp_expense_type, :channel_id, :vim_doc_type_desc, :payment_method,
#           :vendor_name, :vendor_address, :legal_entity_name, :legal_entity_address
#         )
#         """

#         # Prepare data for batch insert with sanitized parameters
#         transaction_data = []
#         for _, row in group_df.iterrows():
#             data = {
#                 'transaction_id': row.get('TRANSACTION_ID'),
#                 'payment_method': row.get('PAYMENT_METHOD'),
#                 'vendor_name': row.get('VENDOR_NAME'),
#                 'vendor_address': row.get('VENDOR_ADDRESS'),
#                 'legal_entity_name': row.get('NAME'),
#                 'legal_entity_address': row.get('ADDRESS'),
#                 'account_doc_id': account_doc_id,
#                 'client': row.get('CLIENT'),
#                 'company_code': row.get('COMPANY_CODE'),
#                 'region': row.get('REGION_BSEG'),
#                 'accounting_document_number': row.get('DOCUMENT_NUMBER'),
#                 'fiscal_year': row.get('FISCAL_YEAR'),
#                 'item': row.get('ITEM'),
#                 'line_item_id': row.get('LINE_ITEM_ID'),
#                 'debit_credit_indicator': row.get('DEBIT_CREDIT_INDICATOR'),
#                 'gl_account_number': row.get('GL_ACCOUNT_NUMBER'),
#                 'lineitem_amount_in_local_currency': row.get('LINEITEM_AMOUNT_IN_LOCAL_CURRENCY'),
#                 'lineitem_amount_in_document_currency': row.get('LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY'),
#                 'purchase_order_number': row.get('PURCHASE_ORDER_NUMBER'),
#                 'po_item_number': row.get('PO_ITEM_NUMBER'),
#                 'item_text': row.get('ITEM_TEXT'),
#                 'supplier_id': row.get('SUPPLIER_ID'),
#                 'document_type': row.get('DOCUMENT_TYPE'),
#                 'invoice_date': row.get('INVOICE_DATE'),
#                 'posted_date': row.get('POSTED_DATE'),
#                 'invoice_number': row.get('INVOICE_NUMBER'),
#                 'payment_date': row.get('Payment date'),
#                 'due_date': row.get('DUE_DATE'),
#                 'payer': row.get('PAYER'),
#                 'reason_code': row.get('REASON_CODE'),
#                 'quarter_label': row.get('QUARTER_LABEL'),
#                 'withhold_tax_item': row.get('WITHHOLD_TAX_ITEM'),
#                 'withhold_tax_type': row.get('WITHHOLD_TAX_TYPE'),
#                 'withhold_tax_code': row.get('WITHHOLD_TAX_CODE'),
#                 'withhold_tax_base_lc': row.get('WITHHOLD_TAX_BASE_LC'),
#                 'withhold_tax_base_fc': row.get('WITHHOLD_TAX_BASE_FC'),
#                 'document_type_description': row.get('DOCUMENT_TYPE_DESCRIPTION'),
#                 'item_category': row.get('Item Category'),
#                 'payment_terms_po': row.get('PAYMENT_TERMS_PO'),
#                 'po_currency': row.get('PO_CURRENCY'),
#                 'exchange_rate_po': row.get('EXCHANGE_RATE_PO'),
#                 # 'purchasing_document_date': row.get('PURCHASING_DOCUMENT_DATE'),
#                 'supplying_vendor': row.get('SUPPLYING_VENDOR'),
#                 'invoicing_party': row.get('INVOICING_PARTY'),
#                 'down_payment_indicator': row.get('DOWN_PAYMENT_INDICATOR'),
#                 'vat_registration_number': row.get('VAT_REGISTRATION_NUMBER'),
#                 'payee': row.get('PAYEE'),
#                 'dow_gstin': row.get('DOW_GSTIN'),
#                 'vim_dp_document_type': row.get('VIM_DP_DOCUMENT_TYPE'),
#                 'vim_dp_expense_type': row.get('VIM_DP_EXPENSE_TYPE'),
#                 'channel_id': row.get('CHANNEL_ID'),
#                 'vim_doc_type_desc': row.get('VIM_DOC_TYPE_DESC')
#             }
#             # Sanitize each transaction record
#             transaction_data.append(sanitize_parameters(data))

#         # Batch insert all transaction rows
#         if transaction_data:
#             conn.execute(text(insert_sql), transaction_data)
#             log_message(f"Inserted {len(transaction_data)} transaction rows for account_doc_id {account_doc_id} into {table_name}")
