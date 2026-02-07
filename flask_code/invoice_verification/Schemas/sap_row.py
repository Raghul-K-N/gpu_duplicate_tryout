from typing import Dict, List, Tuple, Any
import os
import pandas as pd
import re
from lxml import etree

from invoice_verification.logger.logger import log_message

class SAPRow:
    """Data class to hold SAP row data and extracted file contents"""
    
    def __init__(self,
                account_doc_id: int,
                group: pd.DataFrame):
        
        # variables for logging
        self.non_available_fields = []

        # SAP Data fields
        self.tenant_code = self.safe_get(group, "CLIENT")
        self.transaction_id = self.safe_get(group, "TRANSACTION_ID")
        self.transaction_count = len(group)
        self.account_document_id = self.safe_get(group, "ACCOUNT_DOC_ID")
        self.account_document_number = self.safe_get(group, "DOCUMENT_NUMBER")
        self.fiscal_year = self.safe_get(group, "FISCAL_YEAR")
        self.region = self.safe_get(group, "REGION_BSEG")
        self.doc_type = self.safe_get(group, "DOCUMENT_TYPE")
        self.dp_doc_type = self.safe_get(group, "VIM_DP_DOCUMENT_TYPE")
        self.vim_comments = self.safe_get(group, "VIM_COMMENTS")
        self.transaction_code = self.safe_get(group, "TRANSACTION_CODE")
        self.doc_type_description = self.safe_get(group, "DOCUMENT_TYPE_DESCRIPTION")
        self.dp_doc_type_description = self.safe_get(group, "VIM_DOC_TYPE_DESC")
        self.vendor_code = self.safe_get(group, "SUPPLIER_ID")
        self.assignment = self.safe_get(group, "ASSIGNMENT")
        self.inv_type = self.safe_get(group, "INVOICE_TYPE")
        self.gl_account_name = self.safe_get(group, "GL_ACCOUNT_NAME")
        self.cost_centre = self.safe_get(group, "COST_CENTRE")
        self.invoice_number = self.safe_get(group, "INVOICE_NUMBER")
        self.company_code = self.safe_get(group, "COMPANY_CODE")
        self.invoice_amount = self.safe_get(group, "TOTAL_AMOUNT")
        self.invoice_amount_usd = self.safe_get(group,'TOTAL_AMOUNT_USD')
        self.invoice_currency = self.safe_get(group, "DOCUMENT_CURRENCY")
        self.local_currency = self.safe_get(group, "LOCAL_CURRENCY")
        self.total_amount_lc = self.safe_get(group, "TOTAL_AMOUNT_LC")
        self.total_amount_lc_usd = self.safe_get(group,'TOTAL_AMOUNT_LC_USD')
        self.vendor_name = self.safe_get(group, "VENDOR_NAME")
        self.vendor_address = self.safe_get(group, "VENDOR_ADDRESS")
        self.payment_method = self.safe_get(group, "PAYMENT_METHOD")
        self.payment_method_description = self.safe_get(group, "PAYMENT_METHOD_DESCRIPTION")
        self.payment_method_supplement = self.safe_get(group, "PAYMENT_METHOD_SUPPLEMENT")
        self.payee_address = self.safe_get(group, "ALTERNATE_PAYEE")
        self.payee = self.safe_get(group, "PAYEE")
        self.payer = self.safe_get(group, "PAYER")
        # self.bank_account_number = self.safe_get(group, "BANK_ACCOUNT")
        # self.bank_name = self.safe_get(group, "BANK_NAME")
        self.vat_tax_id = self.safe_get(group, "VENDOR_TAX_NUMBER_LIST")
        if self.vat_tax_id != "":
            self.vat_tax_id = self.vat_tax_id.split(",")
        else:
            self.vat_tax_id = []
        # self.bank_account_holder = self.safe_get(group, "ACCOUNT_HOLDER")
        self.legal_entity_name = self.safe_get(group, "LE_NAME")
        self.legal_entity_address = self.safe_get(group, "LE_ADDRESS")
        self.vendor_account_history = self.safe_get(group, "VENDOR_ACCOUNT_HISTORY")
        self.exchange_rate = self.safe_get(group, "EXCHANGE_RATE")
        self.exchange_rate_usd = self.safe_get(group, "EXCHANGE_RATE_USD")
        self.po_currency = self.safe_get(group, "PO_CURRENCY")
        self.text_field = self.safe_get(group, "HEADER_TEXT")
        self.doc_header_text = self.safe_get(group, "DOCUMENT_HEADER_TEXT")
        self.payment_term_reason_code = self.safe_get(group, "REASON_CODE")
        self.payment_term_description = self.safe_get(group, "PAYMENT_TERMS_DESCRIPTION")
        self.reason_code_description = self.safe_get(group, "REASON_CODE_DESCRIPTION")
        self.payment_terms = self.safe_get(group, "PAYMENT_TERMS_Invoice")
        self.invoice_date = self.safe_get(group, "INVOICE_DATE")
        self.invoice_receipt_date = self.safe_get(group, "INVOICE_RECEIPT_DATE")
        self.posted_date = self.safe_get(group, "POSTED_DATE")
        self.entered_date = self.safe_get(group, "ENTERED_DATE")
        self.entered_by = self.safe_get(group, "ENTERED_BY")
        self.due_date = self.safe_get(group, "DUE_DATE")
        self.clearing_document_number = self.safe_get(group, "CLEARING_DOCUMENT_NUMBER")
        self.debit_credit_indicator = self.safe_get(group, "DEBIT_CREDIT_INDICATOR")
        self.reverse_document_number = self.safe_get(group, "REVERSE_DOCUMENT_NUMBER")
        self.ref_transaction = self.safe_get(group, "REF_TRANSACTION")
        self.reference_key = self.safe_get(group, "REFERENCE_KEY")
        self.partner_bank_type = self.safe_get(group, "PARTNER_BANK_TYPE")
        self.transaction_type = self.safe_get(group, "VIM_DP_TRANSACTION_EVENT")
        self.condition_type = self.safe_get(group, "CONDITION_TYPE")
        self.doa_type = self.safe_get(group, "DOA_TYPE")
        self.expense_type = self.safe_get(group, "VIM_DP_EXPENSE_TYPE")
        self.grc_doc_type = self.safe_get(group, "GRC_DOC_TYPE")
        self.vendor_tax_id = self.safe_get(group, "VENDOR_VAT_REG_NO")
        self.vendor_gst_in_number = self.safe_get(group, "VENDOR_GSTIN")
        self.dow_gst_in_number = self.safe_get(group, "DOW_GSTIN")
        self.vat_amount = self.safe_get(group, "TAX_AMOUNT")
        self.gst_amount = self.safe_get(group, "GST_AMOUNT")
        self.wht_amount = self.safe_get(group, "WHT_AMOUNT")
        self.wht_type = self.safe_get(group, "WITHHOLD_TAX_TYPE")
        self.wht_code = self.safe_get(group, "WITHHOLD_TAX_CODE")
        self.wht_item = self.safe_get(group, "WITHHOLD_TAX_ITEM")
        self.wht_base_lc = self.safe_get(group, "WITHHOLD_TAX_BASE_LC")
        self.wht_base_fc = self.safe_get(group, "WITHHOLD_TAX_BASE_FC")
        self.esr_number = self.safe_get(group, "ESR_NUMBER")
        # self.udc_amount = self.safe_get(group, "UDC_AMOUNT")
        self.opentext_user_id = self.safe_get(group, "OPENTEXT_USER_ID_APR")
        self.app_action = self.safe_get(group, "APP_ACTION_APR")
        self.quarter_label = self.safe_get(group, "QUARTER_LABEL")
        self.channel_id = self.safe_get(group, "CHANNEL_ID")
        self.order_type = self.safe_get(group, "ORDER_TYPE")

        self.po_accounting_group = self.safe_get(group, "ACCOUNTING_GROUP")

        # Vendor Details
        # self.bank_account_number = group["BANK_ACCOUNT"].to_list() if "BANK_ACCOUNT" in group.columns and not group.empty else []
        # self.bank_name = group["BANK_NAME"].to_list() if "BANK_NAME" in group.columns and not group.empty else []

        # Line Item Level Data
        self.item_category = list(group["ITEM_CATEGORY"].to_list()) if "ITEM_CATEGORY" in group.columns and not group.empty else []
        self.po_type = list(group["DOCUMENT_TYPE_PO"].to_list()) if "DOCUMENT_TYPE_PO" in group.columns and not group.empty else []
        self.po_number = list(group["PURCHASE_ORDER_NUMBER"].to_list()) if "PURCHASE_ORDER_NUMBER" in group.columns and not group.empty else []
        self.line_item_number = list(group["LINE_ITEM_ID"].to_list()) if "LINE_ITEM_ID" in group.columns and not group.empty else []
        self.line_item_amount = list(group["LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY"].to_list()) if "LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY" in group.columns and not group.empty else []
        self.gl_account_number = list(group["GL_ACCOUNT_NUMBER"].to_list()) if "GL_ACCOUNT_NUMBER" in group.columns and not group.empty else []
        if self.gl_account_number != [] and len(self.gl_account_number)>0:
            self.gl_account_number = self.gl_account_number[1:]

        # New SAP Fields - Additional Header/Line Item Data
        self.item = self.safe_get(group, "Item")
        self.po_item_number = self.safe_get(group, "PO_ITEM_NUMBER")
        # self.item_text = self.safe_get(group, "ITEM_TEXT")
        self.region_bkpf = self.safe_get(group, "REGION_BKPF")
        self.debit_credit_indicator_header = self.safe_get(group, "DEBIT_CREDIT_INDICATOR_HEADER_LEVEL")
        self.payment_date = self.safe_get(group, "PAYMENT_DATE")
        self.header_text = self.safe_get(group, "HEADER_TEXT")
        self.year = self.safe_get(group, "YEAR")
        self.payment_block = self.safe_get(group, "PAYMENT_BLOCK")
        self.baseline_date = self.safe_get(group, "BASELINE_DATE")
        self.line_item_amount_lc = self.safe_get(group, "LINEITEM_AMOUNT_IN_LOCAL_CURRENCY")

        self.line_item_amount_lc_usd  = self.safe_get(group,"LINEITEM_AMOUNT_IN_LOCAL_CURRENCY_USD")
        self.line_item_amount_usd = list(group["LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY_USD"].to_list()) if "LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY_USD" in group.columns and not group.empty else []


        # Purchase Order Header (EKKO) Data
        self.company_code_po = self.safe_get(group, "COMPANY_CODE_PO")
        self.document_category = self.safe_get(group, "DOCUMENT_CATEGORY")
        self.document_type_po = self.safe_get(group, "DOCUMENT_TYPE_PO")
        self.deletion_indicator_ekko = self.safe_get(group, "DELETION_INDICATOR_EKKO")
        self.created_on = self.safe_get(group, "CREATED_ON")
        self.supplier_id_po = self.safe_get(group, "SUPPLIER_ID_PO")
        self.payment_terms_po = self.safe_get(group, "PAYMENT_TERMS_PO")
        self.exchange_rate_po = self.safe_get(group, "EXCHANGE_RATE_PO")
        self.purchasing_document_date = self.safe_get(group, "PURCHASING_DOCUMENT_DATE")
        self.supplying_vendor = self.safe_get(group, "SUPPLYING_VENDOR")
        self.invoicing_party = self.safe_get(group, "INVOICING_PARTY")
        self.down_payment_indicator = self.safe_get(group, "DOWN_PAYMENT_INDICATOR")
        self.vat_registration_number = self.safe_get(group, "VAT_REGISTRATION_NUMBER")
        self.appl_obj_change_ekko = self.safe_get(group, "Appl.obj.change_EKKO")
        self.co_code_posting_block = self.safe_get(group, "CO_CODE_POSTING_BLOCK")
        self.co_code_deletion_flag = self.safe_get(group, "CO_CODE_DELETION_FLAG")
        self.purchasing_org = self.safe_get(group, "PURCHASING_ORG")

        # Purchase Order Item (EKPO) Data
        self.deletion_indicator_ekpo = self.safe_get(group, "DELETION_INDICATOR_EKPO")
        self.short_text_po = self.safe_get(group, "SHORT_TEXT_PO")
        self.po_quantity = self.safe_get(group, "PO_QUANTITY")
        self.order_unit = self.safe_get(group, "ORDER_UNIT")
        self.net_price = self.safe_get(group, "NET_PRICE")
        self.gross_value = self.safe_get(group, "GROSS_VALUE")
        self.goods_receipt = self.safe_get(group, "GOODS_RECEIPT")
        self.evaluated_receipt_settlement = self.safe_get(group, "EVALUATED_RECEIPT_SETTLEMENT")
        self.origin = self.safe_get(group, "ORIGIN")
        self.origin_region = self.safe_get(group, "ORIGIN_REGION")
        self.destination_country = self.safe_get(group, "DESTINATION_COUNTRY")
        self.destination_region = self.safe_get(group, "DESTINATION_REGION")
        self.appl_obj_change_ekpo = self.safe_get(group, "Appl.obj.change_EKPO")

        # Vendor Master (LFB1) - Company Code Level
        self.payment_terms_vendor_master = self.safe_get(group, "PAYMENT_TERMS_VendorMaster")
        self.clerks_internet = self.safe_get(group, "CLERKS_INTERNET")
        self.payment_methods_vendor = self.safe_get(group, "Payment methods")
        self.tolerance_group = self.safe_get(group, "TOLERANCE_GROUP")
        self.vendor_co_code_created_on = self.safe_get(group, "VENDOR_CO_CODE_CREATED_ON")
        self.appl_obj_change_lfb1 = self.safe_get(group, "Appl.obj.change_LFB1")

        # Vendor Master (LFA1) - General Data
        self.vendor_country = self.safe_get(group, "VENDOR_COUNTRY")
        self.vendor_name_2 = self.safe_get(group, "VENDOR_NAME_2")
        self.vendor_name_3 = self.safe_get(group, "VENDOR_NAME_3")
        self.vendor_name_4 = self.safe_get(group, "VENDOR_NAME_4")
        self.vendor_city = self.safe_get(group, "VENDOR_CITY")
        self.vendor_po_box = self.safe_get(group, "VENDOR_PO_BOX")
        self.vendor_postal_code = self.safe_get(group, "VENDOR_POSTAL_CODE")
        self.vendor_region = self.safe_get(group, "VENDOR_REGION")
        self.vendor_street = self.safe_get(group, "VENDOR_STREET")
        self.vendor_deletion_flag = self.safe_get(group, "DELETION_FLAG")
        self.vendor_tax_number_1 = self.safe_get(group, "VENDOR_TAX_NUMBER_1")
        self.vendor_tax_number_2 = self.safe_get(group, "VENDOR_TAX_NUMBER_2")
        self.vendor_tax_number_3 = self.safe_get(group, "VENDOR_TAX_NUMBER_3")
        self.vendor_tax_number_4 = self.safe_get(group, "VENDOR_TAX_NUMBER_4")
        self.vendor_tax_number_5 = self.safe_get(group, "VENDOR_TAX_NUMBER_5")
        self.vendor_tax_number_6 = self.safe_get(group, "VENDOR_TAX_NUMBER_6")
        self.vendor_telephone_1 = self.safe_get(group, "VENDOR_TELEPHONE_1")
        self.vendor_telephone_2 = self.safe_get(group, "VENDOR_TELEPHONE_2")
        self.vendor_vat_reg_no = self.safe_get(group, "VENDOR_VAT_REG_NO")
        self.vendor_alternate_payee = self.safe_get(group, "VENDOR_ALTERNATE_PAYEE")
        self.address_vendor_master = self.safe_get(group, "ADDRESS_VendorMaster")
        self.vendor_language = self.safe_get(group, "LANGUAGE")
        self.vendor_created_on = self.safe_get(group, "VENDOR_CREATED_ON")
        self.appl_obj_change_lfa1 = self.safe_get(group, "Appl.obj.change_LFA1")

        # Bank Details
        self.bank_country = self.safe_get(group, "BANK_COUNTRY")
        self.bank_key = self.safe_get(group, "BANK_KEY")
        self.partner_bank_type_vendor_master = self.safe_get(group, "PARTNER_BANK_TYPE_VendorMaster")
        self.appl_obj_change = self.safe_get(group, "APPL_OBJ_CHANGE")

        # VIM (Vendor Invoice Management) Data
        self.vim_document_id = self.safe_get(group, "VIM_DOCUMENT_ID")
        self.vim_document_status = self.safe_get(group, "VIM_DOCUMENT_STATUS")
        self.vim_special_status = self.safe_get(group, "VIM_SPECIAL_STATUS")
        self.vim_dp_transaction_event = self.safe_get(group, "VIM_DP_TRANSACTION_EVENT")
        self.vim_object_type = self.safe_get(group, "VIM_OBJECT_TYPE")
        self.vim_object_key = self.safe_get(group, "VIM_OBJECT_KEY")
        self.vim_doc_status_desc = self.safe_get(group, "VIM_DOC_STATUS_DESC")
        self.vim_1log_comments = self.safe_get(group, "VIM_1LOG_COMMENTS")
        self.vim_8log_comments = self.safe_get(group, "VIM_8LOG_COMMENTS")
        self.scan_location = self.safe_get(group, "SCAN_LOCATION")

        # Additional Invoice/Legal Entity Data
        self.address_invoice = self.safe_get(group, "ADDRESS_Invoice")
        self.le_name_2 = self.safe_get(group, "LE_NAME_2")
        self.le_name_3 = self.safe_get(group, "LE_NAME_3")
        self.le_name_4 = self.safe_get(group, "LE_NAME_4")
        self.country = self.safe_get(group, "COUNTRY")

        # Missing others
        self.tax_code = self.safe_get(group, "TAX_CODE")
        self.audit_reason = self.safe_get(group, "AUDIT_REASON")
        self.udc_amount = self.safe_get(group, "AMOUNT_UDC")
        self.udc_amount_usd = list(group["AMOUNT_UDC_USD"].to_list()) if "AMOUNT_UDC_USD" in group.columns and not group.empty else []

        self.udc_condition_type = list(group["CONDITION_TYPE_UDC"].to_list()) if "CONDITION_TYPE_UDC" in group.columns and not group.empty else []

        
        self.material_ids = list(group["PO_MATERIAL"].to_list()) if "PO_MATERIAL" in group.columns and not group.empty else []
        
        # File paths
        # self.base_path =  str(os.getenv('ATTACHMENTS_FOLDER_PATH'))
        base_path = os.getenv('UPLOADS', '/app/uploads')
        master_folder_name = os.getenv('MASTER_FOLDER', 'dow_transformation')
        attachments_folder_name = os.getenv('ATTACHMENTS_FOLDER', 'attachments')
        attachments_path = os.path.join(base_path, master_folder_name, attachments_folder_name)
        self.base_path = attachments_path
        self.attachments_path, self.attachments = self._get_file_paths()

        # EFT
        self.eft_data = self._get_eft_data()
        self.eft_present_flag = True if self.eft_data else False

        # Invoice type
        self.return_to_vendor = False
        self.resubmitted_invoice = False
        self.elemica_flag = True if str(self.dp_doc_type).strip().upper()=="PO_EL_GLB" else False
        self.ariba_flag = True if str(self.dp_doc_type).strip().upper()=="PO_AN_GLB" else False
        self.script_invoice_flag = False
        from invoice_verification.Parameters.utils import otm_apac_vendor_codes
        if str(self.dp_doc_type).strip().upper() in ["PO_CS_GLB", "PO_FRBLB", "PO_FR_BLB"] and \
                self.channel_id.strip().upper()=="ICC" and any(str(po).strip().upper().startswith("NB") for po in self.po_type) \
                    or str(self.vendor_code).strip() in otm_apac_vendor_codes:
            self.otm_flag = True
        else:
            self.otm_flag = False
        self.ers_flag = True if (str(self.doc_type).strip().upper()=="KD") or (self.ariba_flag and self.doc_type.strip().upper()=="KS") else False

        
        # PDF
        pdf_info, checkbox_radio_mappings = self._get_pdf_file_info()
        self.voucher_no_invoice_checkbox = checkbox_radio_mappings.get("voucher_no_invoice")
        self.voucher_payment_method = checkbox_radio_mappings.get("payment_method")
        self.voucher_transaction_type = checkbox_radio_mappings.get("transaction_type")
        self.invoice_pdf_path = pdf_info.get("invoice_pdf_path")
        self.voucher_pdf_path = pdf_info.get("voucher_pdf_path")
        self.invoice_copy_flag = pdf_info.get("invoice_copy_flag")
        self.voucher_copy_flag = pdf_info.get("voucher_copy_flag")
        self.payment_certificate_flag = pdf_info.get("payment_certificate_flag")
        self.rental_aggrement_flag = pdf_info.get("rental_aggrement_flag")
        self.bank_statement_flag = pdf_info.get("bank_statement_flag")
        self.dummy_generic_invoice_flag = pdf_info.get("dummy_generic_invoice_flag")
        self.payment_certificate_lines = pdf_info.get("payment_certificate_lines", [])
        self.ticket_copy_lines = pdf_info.get("ticket_lines", [])
        self.ticket_copy_flag = pdf_info.get("ticket_flag")

        # XLSX, XML, EMAIL
        self.xml_file_flag = False
        self.excel_file_flag = False
        self.eml_file_flag = False
        self.xml_paths = []
        self.excel_paths = []
        self.eml_paths = []
        for f in self.attachments_path:
            if str(f).strip().upper().endswith(".XML"):
                self.xml_paths.append(f)
                self.xml_file_flag = True
            elif str(f).strip().upper().endswith(".XLSX") or str(f).strip().upper().endswith(".XLS"):
                self.excel_paths.append(f)
                self.excel_file_flag = True
            elif str(f).strip().upper().endswith(".EML") or str(f).strip().upper().endswith(".MSG"):
                self.eml_paths.append(f)
                self.eml_file_flag = True

        # Extracted lines from files (loaded during init)
        # self.invoice_lines = pdf_info.get("invoice_copy_lines", [])
        self.invoice_lines_1 = pdf_info.get("invoice_copy_lines_1", [])
        self.invoice_lines_2 = pdf_info.get("invoice_copy_lines_2", [])
        self.invoice_lines_3 = pdf_info.get("invoice_copy_lines_3", [])
        self.invoice_lines_4 = pdf_info.get("invoice_copy_lines_4", [])
        self.invoice_lines_5 = pdf_info.get("invoice_copy_lines_5", [])
        self.invoice_all_lines = self.invoice_lines_1 + self.invoice_lines_2 + self.invoice_lines_3 + self.invoice_lines_4 + self.invoice_lines_5
        self.voucher_lines = pdf_info.get("voucher_copy_lines", [])
        self.xml_lines = self._load_xml_lines() if self.xml_paths else []
        self.eml_lines = []
        self.email_attachments = []
        for eml_path in self.eml_paths:
            eml_lines, email_attachments = self._load_eml_lines_and_attachments(eml_path) if eml_path else ([],[])
            self.eml_lines.append(eml_lines)
            self.email_attachments.extend(email_attachments)
        self.vim_comment_lines = self._parse_vim_comments() if self.vim_comments else []

        # Concatenating Invoice Attachments and Email Attachments
        self.attachments = self.attachments + self.email_attachments

        # Supporting and confidential Documents
        self.confidential_documents = pdf_info.get("confidential_documents")
        self.supporting_documents_present_flag = any((
                                                self.voucher_copy_flag,
                                                self.bank_statement_flag,
                                                self.payment_certificate_flag,
                                                self.rental_aggrement_flag,
                                                # self.dummy_generic_invoice_flag,
                                                self.excel_file_flag,
                                                self.eml_file_flag,
                                                self.xml_file_flag))
        
        self.manual_transaction_type = None #self.get_transaction_type_from_document()

        # if str(self.transaction_type).strip() == "":
        #     self.transaction_type = "INVOICE" if self.debit_credit_indicator.strip().upper() == "H" else "CREDIT MEMO"

        if (str(self.region).strip().upper() in ["NAA","EMEAI","APAC"]) and (str(self.dp_doc_type).strip().upper()=="PO_IU_GLB") and self.invoice_copy_flag:
            self.script_invoice_flag = True
        elif (str(self.region).strip().upper() in ["LAA", "LATAM"]) and (str(self.dp_doc_type).strip().upper() in ["PO_IU_GLB", "PO_IU_BRA"]) and self.invoice_copy_flag:
            self.script_invoice_flag = True
        # TODO: MERGE AND RENAME THE RSEG TRANSACTION TYPE
        # NOTE: SUBSEQUENT CREDIT/DEBIT LOGIC Not required upto February 2026
        # if str(self.transaction_type).strip() == "":
        #     if self.debit_credit_indicator.strip().upper()=="H" and self.rseg_subseq_credit_debit.strip().upper()=="X":
        #         self.transaction_type = "4"
        #     elif self.debit_credit_indicator.strip().upper()=="S" and self.rseg_subseq_credit_debit.strip().upper()=="X":
        #         self.transaction_type = "3"
        #     else:
        #         self.transaction_type = "1" if self.debit_credit_indicator.strip().upper() == "H" else "2"

        log_message(f"SAP Row initialized, no of missing fields: {len(self.non_available_fields)}")
        log_message(f"Missing fields: {self.non_available_fields}") 

    def safe_get(self, df: pd.DataFrame, col: str, default="") -> str:
        """Safely extract a column value from DataFrame, returning default if column doesn't exist or value is null"""
        if col not in df.columns:
            # log_message(f"Column '{col}' not found in DataFrame. Returning default value.",error_logger=True)
            self.non_available_fields.append(col)
            return default
        value = df[col].iloc[0]
        return default if pd.isna(value) or value is None or value == "" else str(value)
    
    def _merge_address_columns(self, group: pd.DataFrame, columns: List[str]) -> str:
            """Merge multiple address columns into a single string"""
            parts = []
            for col in columns:
                value = self.safe_get(group, col)
                if value and value.strip():
                    parts.append(value.strip())
            return ", ".join(parts) if parts else ""


    def get_transaction_type_from_document(self) -> str:
        """Determine transaction type based on document data"""
        
        # if self.voucher_copy_flag:
        #     all_lines_text = " ".join(self.voucher_lines)
        #     log_message("Voucher copy is present, so searching for transaction type keywords in voucher copy")
        #     negative_amount_pattern = re.compile(r'r'-\d+\.\d{2}')
        #     if negative_amount_pattern.search(all_lines_text):
        #         log_message("Negative amount found in voucher copy, setting transaction type to CREDIT MEMO")
        #         return "CREDIT MEMO"
        
        if self.invoice_copy_flag:
            log_message("Invoice copy is present, so searching for transaction type keywords in invoice copy")
            from invoice_verification.Parameters.utils import credit_memo_keywords
            all_lines_text = " ".join(self.invoice_all_lines).lower()
            for keyword in credit_memo_keywords.get(str(self.region).strip().upper(), []):
                if keyword.lower() in all_lines_text:
                    log_message(f"Found credit memo keyword '{keyword}' in invoice copy, setting transaction type to CREDIT MEMO")
                    return "CREDIT MEMO"
                
        log_message("No manual transaction type found from documents")
        return ""


    def _load_xml_lines(self) -> List[str]:
        """
        Reads multiple XML files, extracts element tags, attributes, and text,
        and returns a single list of formatted strings from all files.
        """
        try:
            if not self.xml_paths:
                return []

            # 1. Initialize the accumulator list OUTSIDE the loop.
            # This list will hold the lines from ALL XML files.
            all_lines: List[str] = []

            for path in self.xml_paths:
                log_message(f"Processing XML file: {path}")
                try:
                    # 1. Configure a "Forgiving" Parser
                    # recover=True: Fixes broken tags/structure (Browser behavior)
                    # encoding='utf-8': Forces UTF-8 handling for special chars (like the 'N.Α.' in your file)
                    parser = etree.XMLParser(recover=True, encoding='utf-8', remove_blank_text=True)
                    
                    # 2. Parse
                    tree = etree.parse(path, parser)
                    
                    # 3. Iterate all elements (Depth First)
                    for elem in tree.iter():
                        # Clean the tag name (Remove {Namespace} junk)
                        # content like '{http://www.w3.org/2000/09/xmldsig#}Signature' becomes 'Signature'
                        clean_tag = etree.QName(elem).localname
                        
                        # Build the attribute string
                        # We sort them so they are consistent
                        if elem.attrib:
                            attrs = " ".join(f'{k}="{v}"' for k, v in sorted(elem.attrib.items()))
                            line = f"{clean_tag} {attrs}"
                        else:
                            line = clean_tag

                        # 4. Append Text (Cleaned)
                        # checks for text content and ensures it's not just whitespace
                        if elem.text and elem.text.strip():
                            line += f" {elem.text.strip()}"

                        all_lines.append(line)

                except Exception as e:
                    log_message(f"CRITICAL ERROR processing '{path}': {e}")
            log_message(f"XML Lines Extracted: {len(all_lines)} lines and All lines \n {all_lines}")
            return all_lines
        except Exception as e:
            log_message(f"Error processing XML files path:{self.xml_paths}, Error: {e}", error_logger=True)
            return []


    def _load_eml_lines_and_attachments(self, path) -> Tuple[List[str], List[str]]:
        """
        Parse an .eml or .msg file and return:
        - lines: headers + body text + all Date headers (list of strings)
        - saved_files: list of file paths where attachments were saved
        """
        import email
        from email import policy
        import os
        import re
        import extract_msg

        try:
            eml_path = path

            if eml_path is None or not os.path.exists(eml_path):
                log_message(f"Error: File not found at {eml_path}")
                return [], []

            save_dir = os.path.join(self.base_path, "email_attachments")
            
            # Build filename reference for attachments
            company_code_string = str(self.company_code).strip().upper()
            acc_doc_string = str(self.account_document_number).strip().upper()
            year_string = str(self.fiscal_year).strip().upper()
            
            if company_code_string or acc_doc_string or year_string:
                filename_reference = f"_{company_code_string}_{acc_doc_string}_{year_string}"
            else:
                filename_reference = ""

            lines: List[str] = []
            saved_files: List[str] = []

            # Ensure save directory exists
            os.makedirs(save_dir, exist_ok=True)

            file_extension = os.path.splitext(eml_path)[1].lower()

            if file_extension == '.msg':
                # ===== Handle .msg files =====
                log_message(f"Parsing MSG file: {eml_path}")
                msg = extract_msg.Message(eml_path)

                # Extract headers
                if msg.subject:
                    lines.append(f"Subject: {msg.subject}")
                if msg.sender:
                    lines.append(f"From: {msg.sender}")
                if msg.to:
                    lines.append(f"To: {msg.to}")
                if msg.cc:
                    lines.append(f"CC: {msg.cc}")
                if msg.date:
                    lines.append(f"Sent: {msg.date}")

                # Extract body
                body = msg.body
                if body:
                    lines.extend([line.strip() for line in body.splitlines() if line.strip()])

                # Extract attachments
                # for attachment in msg.attachments:
                #     filename = attachment.longFilename or attachment.shortFilename
                #     if filename:
                #         log_message(f"Saving attachment: {filename}")
                #         save_filename = filename_reference + "_" + str(filename)
                #         filepath = os.path.join(save_dir, save_filename)
                #         attachment.save(customPath=save_dir, customFilename=save_filename)
                #         saved_files.append(filepath)

                msg.close()

            elif file_extension == '.eml':
                # ===== Handle .eml files =====
                log_message(f"Parsing EML file: {eml_path}")
                
                with open(eml_path, 'r', encoding='utf-8', errors='ignore') as f:
                    raw_content = f.read()
                    f.seek(0)
                    msg = email.message_from_file(f, policy=policy.default)

                # Extract subject and headers
                if msg['subject']:
                    lines.append(f"Subject: {msg['subject']}")
                if msg['from']:
                    lines.append(f"From: {msg['from']}")
                if msg['to']:
                    lines.append(f"To: {msg['to']}")
                if msg['cc']:
                    lines.append(f"CC: {msg['cc']}")

                # Extract *all* Date headers (for email chains)
                date_headers = re.findall(r'^\s*Date:\s*(.+)$', raw_content, flags=re.MULTILINE | re.IGNORECASE)
                for d in date_headers:
                    lines.append(f"Sent: {d.strip()}")

                # Extract body and attachments
                if msg.is_multipart():
                    for part in msg.walk():
                        cdisp = part.get_content_disposition()
                        ctype = part.get_content_type()

                        # if cdisp == "attachment":
                        #     filename = part.get_filename()
                        #     if filename:
                        #         log_message(f"Saving attachment: {filename}")
                        #         save_filename = filename_reference + "_" + str(filename)
                        #         filepath = os.path.join(save_dir, save_filename)
                        #         with open(filepath, "wb") as f_out:
                        #             payload = part.get_payload(decode=True)
                        #             if isinstance(payload, bytes):
                        #                 f_out.write(payload)
                        #         saved_files.append(filepath)

                        if ctype == "text/plain" and cdisp != "attachment":
                            body = part.get_content()
                            lines.extend([line.strip() for line in body.splitlines() if line.strip()])
                else:
                    body = msg.get_content()
                    lines.extend([line.strip() for line in body.splitlines() if line.strip()])
            
            else:
                log_message(f"Error: Unsupported file format '{file_extension}'. Use .eml or .msg files.")
                return [], []

            return lines, saved_files
        except Exception as e:
            log_message(f"Error processing email file '{path}': {e}", error_logger=True)
            return [], []

    
    def _parse_vim_comments(self) -> List:
        """Parse VIM comments into structured lines"""
        # Split vim comments by newlines or delimiters
        return self.vim_comments.split('\n')
    
    def _get_file_paths(self) -> Tuple[List,List]:
        """Get File Paths"""

        # Company code can be left striped of zeros so we should handle by filling zeros to left
        company_code_string = str(self.company_code).strip().upper().zfill(4)
        acc_doc_string = str(self.account_document_number).strip().upper()
        year_string = str(self.fiscal_year).strip().upper()
        # format = f"{company_code_string}_{acc_doc_string}_{year_string}"
        # Search for this pattern _accdocstring_fiscalyear
        format = f"SAPS4_{acc_doc_string}_{company_code_string}_"
        from invoice_verification.logger.logger import log_message
        import os

        # attachment_path = os.getenv('ATTACHMENTS_FOLDER_PATH')
        base_path = os.getenv('UPLOADS', '/app/uploads')
        master_folder_name = os.getenv('MASTER_FOLDER', 'dow_transformation')
        attachments_folder_name = os.getenv('ATTACHMENTS_FOLDER', 'attachments')
        attachment_path = os.path.join(base_path, master_folder_name, attachments_folder_name)
        log_message(f"Attachment Path from ENV: {attachment_path}")
        if attachment_path is None:
            return [], []
        attachments_with_basepath = [os.path.join(attachment_path,f) for f in os.listdir(attachment_path) if f.startswith(format)]
        attachements = [f for f in os.listdir(attachment_path) if f.startswith(format)]

        return attachments_with_basepath, attachements
    
    def _get_eft_data(self):
        """
        This function return a  
        with values on :
            1. EFT data if not present None
        """
        # TODO: after getting EFT data
        pass


    def _get_pdf_file_info(self) -> Tuple[Dict, Dict[str, Any]]:
        """
        Check and Return the File type
        Args:
            None
        Returns:
            A dictionary with keys:
                - invoice_copy_lines: List of lines from invoice copy
                - payment_certificate_lines: List of lines from payment certificate
                - voucher_copy_lines: List of lines from voucher copy
                - invoice_pdf_path: Path to the invoice PDF file
                - voucher_pdf_path: Path to the voucher PDF file
                - invoice_copy_flag: Boolean indicating presence of invoice copy
                - voucher_copy_flag: Boolean indicating presence of voucher copy
                - payment_certificate_flag: Boolean indicating presence of payment certificate
                - rental_aggrement_flag: Boolean indicating presence of rental agreement
                - bank_statement_flag: Boolean indicating presence of bank statement
                - dummy_generic_invoice_flag: Boolean indicating presence of dummy/generic invoice
                - confidential_documents: Boolean indicating presence of confidential documents
            A dictionary of checkbox and radio button mappings extracted from PDFs
        """

        from invoice_verification.invoice_extraction import extract_text_lines
        from pdfminer.pdfdocument import PDFPasswordIncorrect
        from invoice_verification.logger.logger import log_message

        try:
            pdf_paths = [f for f in self.attachments_path if f.lower().endswith(".pdf")]
            checkbox_radio_mappings: Dict[str, Any] = {}
            
            if pdf_paths == []:
                return {"invoice_copy_lines_1": [],
                        "invoice_copy_lines_2": [],
                        "invoice_copy_lines_3": [],
                        "invoice_copy_lines_4": [],
                        "invoice_copy_lines_5": [],
                        "payment_certificate_lines": [],
                        "voucher_copy_lines": [],
                        "ticket_lines": [],
                        "invoice_pdf_path": None,
                        "voucher_pdf_path": None,
                        "invoice_copy_flag": False,
                        "voucher_copy_flag": False,
                        "payment_certificate_flag": False,
                        "rental_aggrement_flag": False,
                        "bank_statement_flag": False,
                        "dummy_generic_invoice_flag": False,
                        "ticket_flag": False,
                        "confidential_documents": False}, checkbox_radio_mappings
            
            all_lines: List = []
            invoice_copy_lines_1 = []
            invoice_copy_lines_2 = []
            invoice_copy_lines_3 = []
            invoice_copy_lines_4 = []
            invoice_copy_lines_5 = []
            payment_certificate_lines = []
            voucher_copy_lines = []
            ticket_lines = []
            invoice_pdf_path = None
            voucher_pdf_path = None
            invoice_copy_flag = False
            voucher_copy_flag = False
            payment_certificate_flag = False
            rental_agreement_flag = False
            bank_statement_flag = False
            ticket_flag = False
            dummy_generic_invoice_flag = False
            confidential_documents = False
            log_message(f"PDF Paths found: {pdf_paths}")
            for path in pdf_paths:
                try:
                    all_lines, checkbox_radio_mappings_all = extract_text_lines(account_document=str(self.account_document_number), 
                                                                            file_path=path,
                                                                            return_checkbox_radio_mappings=True,
                                                                            vendor_code=self.vendor_code)
                except PDFPasswordIncorrect:
                    log_message(f"Password required for PDF, so it is a confidential document")
                    confidential_documents = True
                    continue
                except Exception as e:
                    log_message(f"Failed to read PDF due to: {e}",error_logger=True)
                    continue

                # Use classify_with_phrases to identify document type
                from invoice_verification.Parameters.utils import remove_page_markers
                marker_removed_all_lines = remove_page_markers(all_lines)
                processed_all_lines = [str(l).strip() for l in marker_removed_all_lines if str(l).strip() != '']
                
                # Check for dummy/generic/test invoices first
                # NOTE: Only check for the first 5 lines
                for line in all_lines[:5]:
                    line = str(line).lower()
                    if ("test" in line) or ("dummy" in line) or ("generic" in line) or (len(processed_all_lines) < 10):
                        dummy_generic_invoice_flag = True
                        break
                
                if not dummy_generic_invoice_flag:
                    # Define patterns for document classification
                    DOC_TYPE_PATTERNS = {
                        "voucher": re.compile(r'\b(nopo voucher request form|voucher)\b', re.IGNORECASE),
                        "rental": re.compile(r'\b(rental invoice|rental agreement)\b', re.IGNORECASE),
                        "bank_statement": re.compile(r'\b(bank statement)\b', re.IGNORECASE),
                        "payment_certificate": re.compile(r'\b(payment certificate)\b', re.IGNORECASE),
                        'ticket': re.compile(r'\b(ptp details)\b', re.IGNORECASE)
                    }
                    
                    # Check first 5 lines for classification
                    all_lines_text = " ".join(all_lines)
                    
                    doc_type = "invoice"  # default
                    for dtype, pattern in DOC_TYPE_PATTERNS.items():
                        if pattern.search(all_lines_text):
                            doc_type = dtype
                            log_message(f"Document classified as '{doc_type}' based on content.")
                            break

                    if doc_type == "ticket":
                        ticket_lines = all_lines.copy()
                        ticket_flag = True
                    
                    elif doc_type == "voucher":
                        voucher_copy_lines = all_lines.copy()
                        voucher_pdf_path = path
                        voucher_copy_flag = True
                        checkbox_radio_mappings = checkbox_radio_mappings_all
                    elif doc_type == "rental":
                        rental_agreement_flag = True
                        invoice_copy_lines_1 = all_lines.copy()
                        invoice_copy_flag = True
                        invoice_pdf_path = path
                    elif doc_type == "bank_statement":
                        bank_statement_flag = True
                        invoice_copy_lines_1 = all_lines.copy()
                        invoice_copy_flag = True
                        invoice_pdf_path = path
                    elif doc_type == "payment_certificate":
                        payment_certificate_flag = True
                        payment_certificate_lines = all_lines.copy()
                        invoice_copy_lines_1 = all_lines.copy()
                        invoice_copy_flag = True
                        invoice_pdf_path = path
                    else:
                        if invoice_copy_lines_1 == []:
                            invoice_copy_lines_1 = all_lines.copy()
                            invoice_copy_flag = True
                            # invoice_copy_lines = all_lines.copy()
                            invoice_pdf_path = path
                        elif invoice_copy_lines_2 == []:
                            invoice_copy_lines_2 = all_lines.copy()
                        elif invoice_copy_lines_3 == []:
                            invoice_copy_lines_3 = all_lines.copy()
                        elif invoice_copy_lines_4 == []:
                            invoice_copy_lines_4 = all_lines.copy()
                        elif invoice_copy_lines_5 == []:
                            invoice_copy_lines_5 = all_lines.copy()
            
            return {"invoice_copy_lines_1": invoice_copy_lines_1,
                    "invoice_copy_lines_2": invoice_copy_lines_2,
                    "invoice_copy_lines_3": invoice_copy_lines_3,
                    "invoice_copy_lines_4": invoice_copy_lines_4,
                    "invoice_copy_lines_5": invoice_copy_lines_5,
                    "payment_certificate_lines": payment_certificate_lines,
                    "voucher_copy_lines": voucher_copy_lines,
                    "ticket_lines": ticket_lines,
                    "invoice_pdf_path": invoice_pdf_path,
                    "voucher_pdf_path": voucher_pdf_path,
                    "invoice_copy_flag": invoice_copy_flag,
                    "voucher_copy_flag": voucher_copy_flag,
                    "payment_certificate_flag": payment_certificate_flag,
                    "rental_aggrement_flag": rental_agreement_flag,
                    "ticket_flag": ticket_flag,
                    "bank_statement_flag": bank_statement_flag,
                    "dummy_generic_invoice_flag": dummy_generic_invoice_flag,
                    "confidential_documents": confidential_documents}, checkbox_radio_mappings
        
        except Exception as e:
            log_message(f"Error while extracting PDF file info: {e}",error_logger=True)
            import traceback
            log_message(traceback.format_exc(), error_logger=True)
            return {"invoice_copy_lines_1": [],
                    "invoice_copy_lines_2": [],
                    "invoice_copy_lines_3": [],
                    "invoice_copy_lines_4": [],
                    "invoice_copy_lines_5": [],
                    "payment_certificate_lines": [],
                    "voucher_copy_lines": [],
                    "ticket_lines": [],
                    "invoice_pdf_path": None,
                    "voucher_pdf_path": None,
                    "invoice_copy_flag": False,
                    "voucher_copy_flag": False,
                    "payment_certificate_flag": False,
                    "rental_aggrement_flag": False,
                    "ticket_flag": False,
                    "bank_statement_flag": False,
                    "dummy_generic_invoice_flag": False,
                    "confidential_documents": False}, {}