# db/models.py
"""
SQLAlchemy models for quarterly-sharded invoice tables
All tables support quarterly partitioning (q1_2025, q2_2025, etc.)
"""

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column, Integer, String, DateTime, JSON, Enum, Date, DECIMAL, Text,
    Index, PrimaryKeyConstraint, Boolean, text
)
from sqlalchemy import ForeignKey
from datetime import datetime

Base = declarative_base()


# DYNAMIC MODEL FACTORIES FOR QUARTERLY TABLES
def create_invoice_processing_model(quarter_label: str):
    """
    Create InvoiceProcessing model for a specific quarter.
    Args:
        quarter_label: e.g., 'q1_2025'
    Returns:
        SQLAlchemy model class
    """
    class InvoiceProcessing(Base):
        __tablename__ = f'zblock_invoice_processing_{quarter_label}'
        
        # Primary key (auto-increment)
        id = Column('invoice_id', Integer, primary_key=True, autoincrement=True)
        
        # Foreign keys
        transaction_id = Column(Integer, nullable=False, index=True)
        account_document_id = Column(Integer, nullable=False)
        
        # Extracted invoice fields
        invoice_number = Column(String(255))
        invoice_date = Column(Date)
        invoice_amount = Column(String(50))
        invoice_currency = Column(String(255))  # Can store multiple currencies
        
        # Vendor information
        vendor_name = Column(String(255))
        vendor_address = Column(Text)
        
        # Legal entity information
        legal_entity_name = Column(String(255))
        legal_entity_address = Column(Text)
        
        # Payment information
        payment_terms = Column(String(255))
        payment_method = Column(String(100))
        
        # Tax information
        vat_tax_code = Column(String(100))
        vat_tax_amount = Column(String(50))
        
        # Additional fields
        doa = Column(String(255))
        udc = Column(String(255))
        transaction_type = Column(String(255))
        service_invoice_confirmation = Column(String(255))
        
        # Banking details
        bank_name = Column(String(255))
        bank_account_number = Column(String(255))
        bank_account_holder_name = Column(String(255))
        
        # Other fields
        text_info = Column(Text)
        legal_requirement = Column(String(255))
        invoice_receipt_date = Column(DateTime)
        gl_account_number = Column(String(255))
        
        # Timestamps
        created_at = Column(DateTime, default=datetime.now, nullable=False)
        last_updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
        
        # Table constraints
        __table_args__ = (
            Index(f"uk_account_doc_id_{quarter_label}", "account_document_id", unique=True),
        )
        
        def to_dict(self):
            return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
    return InvoiceProcessing


def create_invoice_verification_model(quarter_label: str):
    """
    Create InvoiceVerification model for a specific quarter.
    Stores anomaly detection results (mismatch flags).
    Args:
        quarter_label: e.g., 'q1_2025'
    Returns:
        SQLAlchemy model class
    """
    class InvoiceVerification(Base):
        __tablename__ = f'zblock_invoice_verification_{quarter_label}'
        
        # Primary key
        id = Column('verification_id', Integer, primary_key=True, autoincrement=True)
        
        # Foreign key to processing table
        processing_id = Column('invoice_id', Integer, nullable=False, index=True)
        
        # Reference fields
        transaction_id = Column(Integer, nullable=False, index=True)
        account_document_id = Column(Integer, nullable=False)
        
        # Anomaly flags (19 fields)
        invoice_is_attached_anomaly = Column(Integer)  # TINYINT
        invoice_number_anomaly = Column(Integer)
        gl_account_number_anomaly = Column(Integer)
        invoice_amount_anomaly = Column(Integer)
        invoice_currency_anomaly = Column(Integer)
        vendor_name_and_address_anomaly = Column(Integer)
        legal_entity_name_and_address_anomaly = Column(Integer)
        payment_terms_anomaly = Column(Integer)
        invoice_date_anomaly = Column(Integer)
        text_info_anomaly = Column(Integer)
        legal_requirement_anomaly = Column(Integer)
        doa_anomaly = Column(Integer)
        udc_anomaly = Column(Integer)
        transaction_type_anomaly = Column(Integer)
        vat_tax_code_anomaly = Column(Integer)
        service_invoice_confirmation_anomaly = Column(Integer)
        vendor_banking_details_anomaly = Column(Integer)
        payment_method_anomaly = Column(Integer)
        invoice_receipt_date_anomaly = Column(Integer)
        
        # Timestamps
        created_at = Column(DateTime, default=datetime.now, nullable=False)
        last_updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
        
        # Table constraints
        __table_args__ = (
            Index(f"uk_account_doc_id_{quarter_label}", "account_document_id", unique=True),
        )
        
        def to_dict(self):
            return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
    return InvoiceVerification


def create_invoice_param_config_model(quarter_label: str):
    """
    Create InvoiceParamConfig model for a specific quarter.
    Stores per-invoice x parameter metadata.
    Args:
        quarter_label: e.g., 'q1_2025'
    Returns:
        SQLAlchemy model class
    Uses composite primary key (invoice_id, param_code)
    """
    class InvoiceParamConfig(Base):
        __tablename__ = f'zblock_invoice_param_config_{quarter_label}'
        
        # Composite primary key columns
        invoice_id = Column(Integer, nullable=False)
        param_code = Column(String(100), nullable=False)
        
        # Configuration attributes
        validation_method = Column(
            Enum('AUTOMATED', 'MANUAL', 'COMBINED', name='validation_method_enum'),
            nullable=False,
            server_default=text("'AUTOMATED'")
        )
        editable = Column(Boolean, nullable=False, server_default=text("0"))
        highlight = Column(Boolean, nullable=False, server_default=text("0"))
        supporting_fields = Column(JSON)  # Store supporting details as JSON
        
        # Timestamps
        created_at = Column(DateTime, default=datetime.now)
        last_updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
        
        __table_args__ = (
            PrimaryKeyConstraint('invoice_id', 'param_code', name=f'pk_invoice_param_config_{quarter_label}'),
        )

        def to_dict(self):
            return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
    return InvoiceParamConfig


def create_ui_invoice_flat_model(quarter_label: str):
    """
    Create UIInvoiceFlat model for a specific quarter.
    Denormalized view combining all invoice data for UI.
    Args:
        quarter_label: e.g., 'q1_2025'
    Returns:
        SQLAlchemy model class
    """
    class UIInvoiceFlat(Base):
        __tablename__ = f'zblock_ui_invoice_flat_{quarter_label}'
        
        # Primary key (same as processing table)
        invoice_id = Column(Integer, primary_key=True)
        
        # Reference fields
        transaction_id = Column(Integer, nullable=False, index=True)
        account_document_id = Column(Integer, nullable=False)
        
        # All invoice fields (from processing table)
        invoice_number = Column(String(255))
        invoice_date = Column(Date)
        invoice_amount = Column(String(50))
        invoice_currency = Column(String(255))
        vendor_name = Column(String(255))
        vendor_address = Column(Text)
        legal_entity_name = Column(String(255))
        legal_entity_address = Column(Text)
        payment_terms = Column(String(255))
        payment_method = Column(String(100))
        vat_tax_code = Column(String(100))
        vat_tax_amount = Column(String(50))
        doa = Column(String(255))
        udc = Column(String(255))
        transaction_type = Column(String(255))
        service_invoice_confirmation = Column(String(255))
        bank_name = Column(String(255))
        bank_account_number = Column(String(255))
        bank_account_holder_name = Column(String(255))
        text_info = Column(Text)
        legal_requirement = Column(String(255))
        invoice_receipt_date = Column(DateTime)
        gl_account_number = Column(String(255))
        # All anomaly flags (from verification table)
        invoice_is_attached_anomaly = Column(Integer)
        invoice_number_anomaly = Column(Integer)
        gl_account_number_anomaly = Column(Integer)
        invoice_amount_anomaly = Column(Integer)
        invoice_currency_anomaly = Column(Integer)
        vendor_name_and_address_anomaly = Column(Integer)
        legal_entity_name_and_address_anomaly = Column(Integer)
        payment_terms_anomaly = Column(Integer)
        invoice_date_anomaly = Column(Integer)
        text_info_anomaly = Column(Integer)
        legal_requirement_anomaly = Column(Integer)
        doa_anomaly = Column(Integer)
        udc_anomaly = Column(Integer)
        transaction_type_anomaly = Column(Integer)
        vat_tax_code_anomaly = Column(Integer)
        service_invoice_confirmation_anomaly = Column(Integer)
        vendor_banking_details_anomaly = Column(Integer)
        payment_method_anomaly = Column(Integer)
        invoice_receipt_date_anomaly = Column(Integer)
        
        # Metadata fields (aggregated from param_config)
        editable_fields = Column(JSON)  # List of editable field codes
        highlightable_fields = Column(JSON)  # List of highlighted field codes
        validation_methods = Column(JSON)  # Dict of {field: method}
        supporting_fields = Column(JSON)  # Dict of {field: supporting_details}
        
        # Timestamps
        created_at = Column(DateTime, default=datetime.now)
        last_updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
        # Indexes
        __table_args__ = (
            Index(f"ix_ui_invoice_flat_{quarter_label}_account_document_id", "account_document_id"),
        )
        
        def to_dict(self):
            return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
    return UIInvoiceFlat


def create_invoice_attachments_model(quarter_label: str):
    """
    Create InvoiceAttachments model for a specific quarter.
    Stores file paths for invoice attachments.
    Args:
        quarter_label: e.g., 'q1_2025'
    Returns:
        SQLAlchemy model class
    """
    class InvoiceAttachments(Base):
        __tablename__ = f'zblock_invoice_attachments_{quarter_label}'
        
        # Primary key
        attachment_id = Column(Integer, primary_key=True, autoincrement=True)
        
        # Foreign key
        invoice_id = Column(Integer, nullable=False)
        
        # File information
        file_path = Column(String(500), nullable=False)
        file_type = Column(String(100))  # 'invoice_pdf', 'voucher_pdf', 'xml', 'excel', 'eml'
        
        # Timestamps
        created_at = Column(DateTime, default=datetime.now)
        updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
        # Index for invoice_id
        __table_args__ = (
            Index(f"ix_invoice_attachments_{quarter_label}_invoice_id", "invoice_id"),
        )
        
        def to_dict(self):
            return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
    return InvoiceAttachments


def create_invoice_feedback_history_model(quarter_label: str):
    """
    Create InvoiceFeedbackHistory model for a specific quarter.
    Stores history of user feedback on invoice fields.
    Args:
        quarter_label: e.g., 'q1_2025'
    Returns:
        SQLAlchemy model class
    """
    class InvoiceFeedbackHistory(Base):
        __tablename__ = f'zblock_invoice_feedback_history_{quarter_label}'

        feedback_id = Column(Integer, primary_key=True, autoincrement=True)
        invoice_id = Column(Integer, nullable=False)
        field_name = Column(String(255))
        param_code = Column(String(100))
        old_value = Column(String(1000))
        new_value = Column(String(1000))
        old_anomaly_flag = Column(Boolean)
        new_anomaly_flag = Column(Boolean)
        feedback_comment = Column(String(2000))
        user_id = Column(String(200))
        created_at = Column(DateTime, default=datetime.now)
        updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

        __table_args__ = (
            Index(f"ix_invoice_feedback_history_{quarter_label}_invoice_id", "invoice_id"),
        )

        def to_dict(self):
            return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    return InvoiceFeedbackHistory


def get_quarterly_models(quarter_label: str) -> dict:
    """Get all model classes for a specific quarter."""
    return {
        'processing': create_invoice_processing_model(quarter_label),
        'verification': create_invoice_verification_model(quarter_label),
        'param_config': create_invoice_param_config_model(quarter_label),
        'ui_flat': create_ui_invoice_flat_model(quarter_label),
        'attachments': create_invoice_attachments_model(quarter_label),
        'feedback_history': create_invoice_feedback_history_model(quarter_label)
    }