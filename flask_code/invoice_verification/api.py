
from Ingestor.fetch_data import process_batch_data
from flask import Blueprint, jsonify, g
import pandas as pd
import os
from datetime import datetime
from invoice_verification.db.db_connection import read_table,create_flat_tables_based_on_quarters, init_db, dispose_engine, insert_rows_in_zblock_acc_doc_table, insert_rows_in_zblock_transaction_table, setup_quarterly_tables
from invoice_verification.Schemas.invoice_processing_flow import InvoiceProcessingFlow
from invoice_verification.logger.logger import setup_batch_logger, log_message
from extensions import mysql_uri


invoice_blueprint = Blueprint('invoice', __name__)

@invoice_blueprint.route("/invoice_analysis/<int:batch_id>", methods=['GET'])
def invoice_analysis(batch_id):
    """Main endpoint for invoice analysis"""
    try:
        setup_batch_logger(batch_id=batch_id)
        log_message(f"Starting invoice analysis for batch {batch_id}")
        start_time = datetime.now()
        
        # Set Flask context variables for ZBLOCK pipeline
        g.module_nm = 'ZBLOCK'
        g.erp_id = 1
        g.client_id = 1
        g.batch_id = batch_id
        
        # Initialize database connection in main process
        init_db(
            db_uri=str(mysql_uri),
            use_null_pool=False,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600,
            echo=False
        )
        
        # Load common data
        log_message("Loading common data...")
        vendors_df = read_table('ap_vendorlist')
        vendors_df.rename(columns={'VENDORCODE':'vendor_code','PARTNER_BANK_TYPE':'partner_bank_type'}, inplace=True)
        
        # sap_df = pd.read_csv(r"C:\Users\ShriramSrinivasan\Desktop\dow_transformation\data\uat-dec-5\docs_to_run_for_jan6.csv")
        sap_df = process_batch_data(batch_id=batch_id,client_id=None)
        # sap_df = pd.DataFrame()
        if sap_df is None:
            log_message('SAP dataframe is None, no data to process')
            return jsonify({'status':'error','message':'no Data Found'}),404
        elif sap_df.empty:
            log_message("No SAP data found for batch", error_logger=True)
            return jsonify({'status': 'error', 'message': 'No data found'}), 404
        
        # DEBUG: Check available columns
        log_message(f"Available columns in sap_df: {sap_df.columns.tolist()}")
        
        log_message(f"Loaded {len(sap_df)} SAP records")
        log_message(f"No of accounting docs,{sap_df['ACCOUNT_DOC_ID'].nunique()}")
        log_message(f"Region wise Accounting docs count ,{sap_df.groupby('REGION_BSEG')['ACCOUNT_DOC_ID'].nunique()}")

        nan_str_count  = (sap_df=='nan').sum().sum()
        col_wise_nan_str_count  = (sap_df=='nan').sum()
        # Save only cols count if greater than 0
        col_wise_nan_str_count = col_wise_nan_str_count[col_wise_nan_str_count > 0]
        log_message(f"Count of string 'nan' in sap_df: {nan_str_count}")
        log_message(f"Column-wise count of string 'nan' in sap_df:\n{col_wise_nan_str_count}")
        none_str_count  = (sap_df=='None').sum().sum()
        col_wise_none_str_count  = (sap_df=='None').sum()
        # Save only cols count if greater than 0
        col_wise_none_str_count = col_wise_none_str_count[col_wise_none_str_count > 0]
        log_message(f"Count of string 'None' in sap_df: {none_str_count}")
        log_message(f"Column-wise count of string 'None' in sap_df:\n{col_wise_none_str_count}")

        # TODO: Replace all occurrences of 'nan' and 'None' strings with actual NaN values
        sap_df.replace({'nan': pd.NA, 'None': pd.NA}, inplace=True)

        # Get value counts post fix 
        nan_str_count_post_fix  = (sap_df=='nan').sum().sum()
        none_str_count_post_fix  = (sap_df=='None').sum().sum()
        log_message(f"Post-fix count of string 'nan' in sap_df: {nan_str_count_post_fix}")
        log_message(f"Post-fix count of string 'None' in sap_df: {none_str_count_post_fix}")

        
        # Get quarters and setup tables
        quarters = get_quarters(sap_df)

        setup_quarterly_tables(quarters)
        

        create_flat_tables_based_on_quarters(quarters)

        trans_status = insert_rows_in_zblock_transaction_table(sap_df)
        if not trans_status:
            raise Exception("Failed to insert rows in zblock transaction table")
        acc_doc_status = insert_rows_in_zblock_acc_doc_table(sap_df)
        if not acc_doc_status:
            raise Exception("Failed to insert rows in zblock acc doc table")


        # Insert data into flat tables
        
        # Health check placeholder
        # health_check_results = perform_health_check(sap_df, vendors_df)
        
        # Process all invoices
        orchestrator = InvoiceProcessingFlow(
            vendors_df=vendors_df,
            sap_df=sap_df,
            batch_id=batch_id
        )
        
        success_count, failed_count, total_count = orchestrator.process_all_invoices()
        
        end_time = datetime.now()
        processing_time = (end_time - start_time)
        
        log_message(f"""Batch {batch_id} complete: {success_count}/{total_count} invoices processed successfully 
                        {failed_count}/{total_count} invoices failed, Time taken: {processing_time}""")
        
        # Cleanup database connection
        dispose_engine()
        
        return jsonify({
            'status': 'success',
            'batch_id': batch_id,
            'total_invoices': total_count,
            'successful': success_count,
            'failed': failed_count,
            'processing_time': str(processing_time)
        }), 200
        
    except Exception as e:
        log_message(f"Error in invoice analysis for batch {batch_id}: {e}", error_logger=True)
        import traceback
        log_message(traceback.format_exc(), error_logger=True)
        dispose_engine()
        return jsonify({
            'status': 'error',
            'batch_id': batch_id,
            'error': str(e)
        }), 500



def get_quarters(sap_df: pd.DataFrame):
    """
    Get list of unique quarters from SAP DataFrame's QUARTER_LABEL column.
    Args:
        sap_df: SAP DataFrame with 'QUARTER_LABEL' column
    Returns:
        List of unique quarter labels (e.g., ['q1_2025', 'q2_2025'])
    """
    try:
        quarters_list = sap_df['QUARTER_LABEL'].dropna().unique().tolist()
        quarters_list = [str(q).replace("-","_") for q in quarters_list]
        log_message(f"Found quarters in batch: {quarters_list}")
        return quarters_list
    
    except Exception as e:
        log_message(f"Error getting quarters from SAP data: {e}", error_logger=True)
        raise

def read_sap_parquet_data(parquet_folder_path, batch_id):
    try:
        import dask.dataframe as dd
        log_message(f"Parquet data folder path is {parquet_folder_path}")
        
        if not os.path.exists(parquet_folder_path):
            return pd.DataFrame()
        files = os.listdir(parquet_folder_path)
        batch_pqt_file_names = [file for file in files if file.endswith(".parquet")]
        log_message(f"Len of parquet files to read :{len(batch_pqt_file_names)}")
        
        data_files_path = [os.path.join(parquet_folder_path, file) for file in batch_pqt_file_names]

        if not data_files_path:
            return pd.DataFrame()
        log_message(f"Parquet data files path list: {data_files_path}")

        ddf = dd.read_parquet(data_files_path)
        log_message(f"Dask DataFrame shape: {ddf.shape[0].compute()}, {ddf.shape[1]}")
        if ddf is not None:
            if 'batch_id' in ddf.columns:
                log_message(f"Head of batch_id column: {ddf['batch_id'].head()}")
            else:
                log_message("Column 'batch_id' does not exist in the DataFrame.")
        else:
            log_message("Dask DataFrame is None.")
        # log_message(f"Dask DataFrame shape: {ddf.shape.compute()}")
        # log_message(f"Head of batch_id column: {ddf['batch_id'].head().compute()}")
        
        # Filter the Dask DataFrame based on the date range
        ddf_filtered = ddf[ddf['batch_id']==batch_id]
        # Convert the filtered Dask DataFrame to a Pandas DataFrame
        data_df = ddf_filtered.compute()

        data_df = data_df.reset_index(drop=True)
        return data_df
    
    except Exception as e:
        log_message(f"Error occured while uploading sap: {str(e)}",error_logger=True)
        return pd.DataFrame()