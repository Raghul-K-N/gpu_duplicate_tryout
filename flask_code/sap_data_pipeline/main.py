import os
import pandas as pd
import numpy as np
from typing import cast

from .doa.vrdoa_rename import VRDOA_RENAME_MAPPING
from .doa.doaredel_rename import DOA_REDEL_RENAME_MAPPING

from .logger_config import setup_logger, get_logger
from .data_loader import read_all_sap_tables
from .invoice_core.build_invoice_core_from_sap import build_invoice_core
from .company_code.company_master_lookup import merge_t001
from .payment_terms.payment_terms_lookup import merge_t052u
from .payment_method.payment_method_lookup import merge_t042z
from .payment_reason_code.payment_reason_code_lookup import merge_t053s
from .purchase_order.purchase_order_details_lookup import merge_po_info
from .vendor_master_core.vendor_master_lookup import build_vendor_master_core
from .vim_data.vim_data_lookup import merge_invoice_line_item_with_vim_data

from dotenv import load_dotenv
load_dotenv()
import os


def read_sap_data_pipeline(z_block=False, folder_path=None):
    logger = setup_logger(batch_id=0)
    logger.info("Starting SAP Data Pipeline")

    try:
    
        sap_data, expected_tables = read_all_sap_tables(z_block=z_block, folder_path=folder_path)
        pipeline = 'Z-Block' if z_block else 'AP'
        logger.info(f"Loaded {len(sap_data)} tables from SAP data for {pipeline} pipeline")
        # Validate all expected tables are present
        loaded_tables = set(sap_data.keys())
        expected_set = set(expected_tables)
        missing_tables = expected_set - loaded_tables
        
        if missing_tables:
            error_msg = f"Data validation failed. Expected {len(expected_tables)} tables, but {len(missing_tables)} are missing: {sorted(missing_tables)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f" All {len(expected_tables)} expected tables loaded successfully")
        logger.info(f"Pipeline mode: {'Z-Block' if z_block else 'AP'}")
        
        for table_name, df in sap_data.items():
            logger.info(f"Table: {table_name}, Data Shape: {df.shape}")

        invoice_core_df = build_invoice_core(
            bseg=sap_data.get('BSEG'),
            bkpf=sap_data.get('BKPF'),
            with_item=sap_data.get('WTH'),
            t003=sap_data.get('T003'),
            retinv=sap_data.get('RETINV'),
            udc=sap_data.get('UDC'),
        )
        logger.info(f"Invoice Core REGION_BSEG value counts:\n{invoice_core_df['REGION_BSEG'].value_counts()}")
        logger.info(f"Invoice Core COMPANY_CODE value counts:\n{invoice_core_df['COMPANY_CODE'].value_counts()}")
        logger.info(f"Invoice Core DataFrame shape: {invoice_core_df.shape}")
        logger.debug(f"Invoice Core columns: {invoice_core_df.columns.tolist()}")


        invoice_with_company_df = merge_t001(
            invoice_df=invoice_core_df,
            t001=sap_data.get('T001')
        )

        logger.info(f"Invoice with Company DataFrame shape: {invoice_with_company_df.shape}")
        logger.debug(f"Invoice with Company columns: {invoice_with_company_df.columns.tolist()}")

        invoice_with_pmt_terms_df = merge_t052u(
            invoice_df=invoice_with_company_df,
            t052u=sap_data.get('T052U')
        ) 
        logger.info(f"Invoice with Payment Terms DataFrame shape: {invoice_with_pmt_terms_df.shape}")
        logger.debug(f"Invoice with Payment Terms columns: {invoice_with_pmt_terms_df.columns.tolist()}")

        invoice_with_pmt_method_df = merge_t042z(
            invoice_df=invoice_with_pmt_terms_df,
            t042z=sap_data.get('T042Z')
        )

        logger.info(f"Invoice with Payment Method DataFrame shape: {invoice_with_pmt_method_df.shape}")
        logger.debug(f"Invoice with Payment Method columns: {invoice_with_pmt_method_df.columns.tolist()}")

        invoice_with_reason_code_df = merge_t053s(
            invoice_df=invoice_with_pmt_method_df,
            t053s=sap_data.get('T053S')
        )

        logger.info(f"Invoice with Payment Reason Code DataFrame shape: {invoice_with_reason_code_df.shape}")
        logger.debug(f"Invoice with Payment Reason Code columns: {invoice_with_reason_code_df.columns.tolist()}")

        invoice_with_po_data_df = merge_po_info(
            invoice_df=invoice_with_reason_code_df,
            ekko_df=sap_data.get('EKKO'),
            ekpo_df=sap_data.get('EKPO')
        )

        logger.info(f"Invoice with PO Data DataFrame shape: {invoice_with_po_data_df.shape}")
        logger.debug(f"Invoice with PO Data columns: {invoice_with_po_data_df.columns.tolist()}")

        invoice_with_vendor_master_data = build_vendor_master_core(
            invoice_level_data=invoice_with_po_data_df,
            lfa1=sap_data.get('LFA1'),
            lfb1=sap_data.get('LFB1'),
            lfbk=sap_data.get('LFBK'),
            lfm1=sap_data.get('LFM1'),
        )

        logger.info(f"Final Merged DataFrame shape with Vendor Master Core: {invoice_with_vendor_master_data.shape}")
        logger.debug(f"Final Merged DataFrame with Vendor Master Core columns: {invoice_with_vendor_master_data.columns.tolist()}")

        res = merge_invoice_line_item_with_vim_data(
            invoice_line_item=invoice_with_vendor_master_data,
            vim_data=sap_data.get('VIM_'),
            vim_t100t=sap_data.get('VIMT100'),
            vim_t0101=sap_data.get('VIMT101'),
            vim_1log_comm=sap_data.get('1LOGCOMM'),
            vim_8log_comm=sap_data.get('8LOGCOMM'),
            vim_1log=sap_data.get('1LOG_'),
            vim_8log=sap_data.get('8LOG_'),
            vim_apr_log=sap_data.get('APRLOG')    
        )

        logger.info(f"Final Merged DataFrame shape with VIM Data: {res.shape}")
        logger.info(f"Final Merged DataFrame with VIM Data columns: {list(res.columns)}")
    
        res.rename(columns={'DOCUMENT_NUMBER_Invoice': 'DOCUMENT_NUMBER'}, inplace=True)

        # Add two columns TRANSACTION_ID and ACCOUNT_ACCOUNT_DOC_ID 
        # Transaction id is just like row id
        # Account document id is combination of COMPANY_CODE + FISCAL_YEAR + ACCOUNTING_DOCUMENT_NUMBER + Client , assign unique id to each unique combination
        res = res.reset_index(drop=True)
        res['TRANSACTION_ID'] = res.index + 1  # Starting from 1
        res['unique_id'] = (
            res['CLIENT'].astype(str) + '_' +
            res['COMPANY_CODE'].astype(str) + '_' +
            res['FISCAL_YEAR'].astype(str) + '_' +
            res['DOCUMENT_NUMBER'].astype(str)
        )
        res['ACCOUNT_DOC_ID'] = res['unique_id'].factorize()[0] + 1  # Starting from 1

        logger.info(f"Added TRANSACTION_ID and ACCOUNT_DOC_ID columns.")
        logger.debug(f" No of rows {len(res)},No . of unique ACCOUNT_DOC_IDs: {res['ACCOUNT_DOC_ID'].nunique()}")
        # Acc doc count region wise
        region_acc_doc_counts = res.groupby('REGION_BSEG')['ACCOUNT_DOC_ID'].nunique()
        logger.info(f"Number of unique ACCOUNT_DOC_IDs by REGION_BSEG:\n{region_acc_doc_counts}")
        # Create TAX_AMOUNT column at account document level
        # For each account_doc_id, sum the LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY where LINE_ITEM_ID is 'T'
        tax_by_doc = res[res['LINE_ITEM_ID'] == 'T'].groupby('ACCOUNT_DOC_ID')['LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY'].sum().reset_index()
        tax_by_doc.rename(columns={'LINEITEM_AMOUNT_IN_DOCUMENT_CURRENCY': 'TAX_AMOUNT'}, inplace=True)
        res = res.merge(tax_by_doc, on='ACCOUNT_DOC_ID', how='left')
        res['TAX_AMOUNT'] = res['TAX_AMOUNT'].fillna(0)
        
        logger.debug(f"Final columns after TAX_AMOUNT creation: {res.columns.tolist()}")
    
        cols_that_cannot_be_null = ['ENTERED_DATE','POSTED_DATE','DUE_DATE','INVOICE_DATE','VENDOR_NAME','SUPPLIER_ID']
        for col in cols_that_cannot_be_null:
            null_count = res[col].isnull().sum()
            if null_count > 0:
                logger.warning(f"Column {col} has {null_count} null values.")
                logger.debug(f"{col} value counts (with nulls):\n{res[col].value_counts(dropna=False).head()}")
            else:
                logger.info(f"Column {col} has no null values.")

        
        # res['VENDOR_NAME'] = res['VENDOR_NAME'].fillna('Vendor 123')
        # res = res[res['DUE_DATE'].notna()]
        # res.to_csv(file_name, index=False)
        # range of data
        logger.info(f"Data range: {res['POSTED_DATE'].min()} to {res['POSTED_DATE'].max()}")
        logger.info(f"Number of account documents: {res['ACCOUNT_DOC_ID'].nunique()}")

        # res = res[res['DUE_DATE'].notna()]

        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        file_name ='Z' if z_block else 'AP'
        file_name += f"_sap_data_pipeline_test_output_{timestamp}.csv"
        res.to_csv(file_name, index=False)
        logger.info(f"Output saved to: {file_name}")

        # null_values = res.isnull().sum()
        # print("Null values in each column:")
        # for col, null_count in null_values.items():
        #     if null_count > 0:
        #         print(f"Column {col} has {null_count} null values.")
        #     else:
        #         print(f"Column {col} has no null values.")

        doa_df = sap_data.get('VRDOA')
        doa_redel_df = sap_data.get('DOAREDEL')

        base_path = os.getenv('UPLOADS', None)
        if doa_df is not None and not doa_df.empty:
            logger.info(f"DOA DataFrame shape: {doa_df.shape}")
            doa_df.rename(columns=VRDOA_RENAME_MAPPING, inplace=True)
            logger.debug(f"DOA DataFrame columns: {doa_df.columns.tolist()}")
            if base_path is None:
                logger.error("Base path for uploads is not set. Cannot save DOA parquet file.")
            else:
                doa_parquet_path = os.getenv('DOA_PARQUET_PATH', None)
                if doa_parquet_path is None:
                    logger.error("DOA_PARQUET_PATH is not set in environment variables.")
                else:
                    doa_filename = "doa_data.parquet"
                    # temporary Fix for GL_ACCOUNT_1 conversion issue
                    if 'GL_ACCOUNT_1' in doa_df.columns:
                        doa_df['GL_ACCOUNT_1'] = pd.to_numeric(doa_df['GL_ACCOUNT_1'], errors='coerce').fillna(0).astype(int)
                    doa_file_path  = os.path.join(base_path, doa_parquet_path.lstrip("/"), doa_filename)
                    # Create directory if it doesn't exist
                    os.makedirs(os.path.dirname(doa_file_path), exist_ok=True)
                    doa_df.to_parquet(doa_file_path,engine='pyarrow', index=False)
                    logger.info(f"DOA data saved to parquet at: {doa_parquet_path}")

        if doa_redel_df is not None and not doa_redel_df.empty:
            logger.info(f"DOA Redelivery DataFrame shape: {doa_redel_df.shape}")
            doa_redel_df.rename(columns=DOA_REDEL_RENAME_MAPPING, inplace=True)
            logger.debug(f"DOA Redelivery DataFrame columns: {doa_redel_df.columns.tolist()}")
            if base_path is None:
                logger.error("Base path for uploads is not set. Cannot save DOA Redelivery parquet file.")
            else:
                doa_parquet_path = os.getenv('DOA_PARQUET_PATH', None)
                if doa_parquet_path is None:
                    logger.error("DOA_PARQUET_PATH is not set in environment variables.")
                else:
                    doa_redel_filename = "doa_redelivery_data.parquet"
                    # temporary Fix for GL_ACCOUNT_1 conversion issue
                    if 'GL_ACCOUNT_1' in doa_redel_df.columns:
                        doa_redel_df['GL_ACCOUNT_1'] = pd.to_numeric(doa_redel_df['GL_ACCOUNT_1'], errors='coerce').fillna(0).astype(int)
                    doa_redel_file_path  = os.path.join(base_path, doa_parquet_path.lstrip("/"), doa_redel_filename)
                    # Create directory if it doesn't exist
                    os.makedirs(os.path.dirname(doa_redel_file_path), exist_ok=True)
                    
                    doa_redel_df.to_parquet(doa_redel_file_path,engine='pyarrow', index=False)
                    logger.info(f"DOA Redelivery data saved to parquet at: {doa_parquet_path}")
            
            
        
        logger.info("SAP Data Pipeline completed successfully")
        return {"status": "success", "message": "Data Stored Successfully","data":res}
    except Exception as e:
        logger.exception(f"Error in SAP Data Pipeline: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return {"status": "failure", "error": str(e)}