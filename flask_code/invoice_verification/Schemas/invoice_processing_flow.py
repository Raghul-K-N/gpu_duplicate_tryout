import time
from invoice_verification.Schemas.sap_row import SAPRow
from invoice_verification.Schemas.overall_process import OverallProcess
from invoice_verification.logger.logger import log_message


class InvoiceProcessingFlow:
    """Main orchestrator for batch invoice processing"""
    
    def __init__(self, vendors_df, sap_df, batch_id):
        self.vendors_df = vendors_df
        self.sap_df = sap_df
        self.batch_id = batch_id
        # Track success/failure
        self.success_count = 0
        self.failed_count = 0
        
    def process_all_invoices(self):
        """Process all invoices in the batch"""
        try:
            for column in ["DOCUMENT_NUMBER","CLIENT","COMPANY_CODE"]:
                self.sap_df[column] = self.sap_df[column].astype(str).str.strip()

            log_message(f"Vendor DF: {self.vendors_df}")

            # # TRANSACTION_ID is a autoincrement id for each row in SAP data
            # self.sap_df["TRANSACTION_ID"] = self.sap_df.index + 1

            total_groups = len(self.sap_df.groupby(["DOCUMENT_NUMBER","CLIENT","COMPANY_CODE","FISCAL_YEAR"]))
            log_message(f"Processing {total_groups} invoice groups")

            for idx, (accounting_doc, group) in enumerate(self.sap_df.groupby(["DOCUMENT_NUMBER","CLIENT","COMPANY_CODE","FISCAL_YEAR"]), 1):
                try:
                    start_time = time.time()
                    # Create SAPRow object
                    log_message(f"START:: Processing transaction {str(group['TRANSACTION_ID'].iloc[0])} and account document: {str(accounting_doc)}")
                    sap_row = SAPRow(account_doc_id=idx,
                                    group=group)
                    log_message(f"SAP Row created for transaction {str(group['TRANSACTION_ID'].iloc[0])} and account document: {str(accounting_doc)}")
                    # log all parameters of sap_row
                    
                    
                    # Define sensitive fields to exclude from logging
                    long_fields = {'invoice_lines','eml_lines', 'voucher_lines','payment_certificate_lines','xml_lines'}

                    # Log only non-sensitive parameters
                    safe_params = {k: v for k, v in sap_row.__dict__.items() if k not in long_fields}
                    log_message(f"SAP Row parameters (excluding sensitive data): {safe_params}")
                    # from invoice_verification.db.db_connection import insert_quarter_rows, insert_transaction_rows
                   
                    # # Insert account document level data
                    # insert_quarter_rows([sap_row])

                    # log_message(f"Inserted quarter rows for transaction {str(group['TRANSACTION_ID'].iloc[0])} and account document: {str(accounting_doc)}")
                   
                    # # Insert transaction level data
                    # insert_transaction_rows(group, idx, sap_row.quarter_label)

                    # log_message(f"Inserted transaction rows for transaction {str(group['TRANSACTION_ID'].iloc[0])} and account document: {str(accounting_doc)}")
                    

                    

                    # Process individual invoice
                    processor = OverallProcess(sap_row, self.vendors_df, self.batch_id)
                    success = processor.main()
                        
                    if success:
                        self.success_count += 1
                    else:
                        self.failed_count += 1
                    
                except Exception as e:
                    log_message(f"Error processing transaction {str(group['TRANSACTION_ID'].iloc[0])} and account document: {str(accounting_doc)},  Error: {e}", error_logger=True)
                    self.failed_count += 1
                    import traceback
                    log_message(traceback.format_exc(), error_logger=True)
                    continue
                end_time = time.time()
                log_message(f"Time taken: {end_time - start_time:.2f} seconds for transaction {str(group['TRANSACTION_ID'].iloc[0])} and account document: {str(accounting_doc)}")
            log_message(f"Process_all_invoices complete: {self.success_count} success, {self.failed_count} failed")
            return self.success_count, self.failed_count, total_groups
            
        except Exception as e:
            log_message(f"Fatal error in process_all_invoices: {e}", error_logger=True)
            raise