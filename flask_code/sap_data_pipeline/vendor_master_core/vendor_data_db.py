import pandas as pd
import os
from sqlalchemy import create_engine, text
from ..logger_config import get_logger
from local_database import get_database_credentials
import mysql.connector

from dotenv import load_dotenv
load_dotenv()

# Database credentials
credentials = get_database_credentials()
DB_USERNAME = credentials["username"]
DB_PASSWORD = credentials["password"]
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
SSL_CA_FILE = os.getenv("SSL_CA")
USE_SSL_CA = os.getenv("USE_SSL_CA", "false").lower() == "true"
connect_args = {'ssl': {'ca': SSL_CA_FILE}} if USE_SSL_CA else {'ssl': None}
ssl_args = {'ssl_ca': SSL_CA_FILE} if USE_SSL_CA else {}


def get_database_engine():
    """
    Create and return a SQLAlchemy engine for database connection
    """
    engine = create_engine(
        f"mysql+pymysql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        connect_args=connect_args
    )
    return engine


def connect_to_database():
    """
    Connect to Database for Fetching Data
    """
    connection = mysql.connector.connect(
        user=DB_USERNAME,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        **ssl_args
    )
    return connection


def _insert_dataframe_to_sql(df, table_name, connection, logger):
    """
    Insert DataFrame into MySQL table using pandas to_sql() with SQLAlchemy engine.
    
    Args:
        df (pd.DataFrame): DataFrame to insert
        table_name (str): Target table name
        connection: SQLAlchemy engine connection object
        logger: Logger instance
    """
    if df.empty:
        logger.info(f"DataFrame is empty, skipping insert to {table_name}")
        return True
    
    try:
        df.to_sql(table_name, con=connection, index=False, if_exists='append')
        logger.info(f"Successfully inserted {len(df)} rows into {table_name}")
        return True
    except Exception as e:
        logger.error(f"Error inserting data into {table_name}: {e}")
        raise


def store_vendor_master_data_in_database(vendor_master_core_df,vendor_list_to_be_updated):
    """
    Store the vendor master core DataFrame into the database table.

    Dont store duplicate data, insert new rows only when they dont exist.
    Args:
        vendor_master_core_df (pd.DataFrame): DataFrame containing vendor master core data.
        vendor_list_to_be_updated (list): List of vendor codes that need to be updated.

    Returns:
        bool: True if data is stored successfully, False otherwise.
    """
    logger = get_logger()
    logger.info(f"Input DataFrame shape: {vendor_master_core_df.shape}")
    logger.info(f"Input DataFrame columns: {vendor_master_core_df.columns.tolist()}")
    logger.info(f"Update information in vendormaster table")
    logger.info(f"Vendors to be updated: {len(vendor_list_to_be_updated)}")
    logger.info(f"Vendor Codes to be updated: {vendor_list_to_be_updated}")
    
    try:
        # Get SQLAlchemy engine for database operations
        engine = get_database_engine()
        logger.info(f"Engine {engine}")
        
        with engine.begin() as connection:
            # Check if table exists and read existing data
            try:
                read_query = 'SELECT * from ap_vendorlist;'
                existing_data = pd.read_sql(read_query, connection)
                logger.info(f"Existing data shape in ap_vendorlist table: {existing_data.shape}")
            except Exception:
                # Table might not exist
                existing_data = pd.DataFrame()
                logger.info("ap_vendorlist table does not exist or is empty")

            column_mapping = {
                'SUPPLIER_ID': 'VENDORCODE',
                'VENDOR_NAME': 'VENDOR_NAME',  # already matches
                'VENDOR_ADDRESS': 'vendor_address', 
                'BANK_ACCOUNT': 'bank_account_number',
                'ACCOUNT_HOLDER': 'bank_account_holder_name',
                'PARTNER_BANK_TYPE': 'PARTNER_BANK_TYPE',  # already matches
                'IBAN': 'IBAN',  # already matches
                'SWIFT/BIC': 'SWIFT_BIC',
                'PAYMENT_TERMS': 'payment_terms'
            }

            # Query apvendorlist table to get existing column names
            columns_in_table = pd.read_sql("SHOW COLUMNS FROM ap_vendorlist", connection)['Field'].tolist()
            logger.info(f"Database table columns: {columns_in_table}")

            
            # Create a copy to avoid SettingWithCopyWarning
            df_to_insert = vendor_master_core_df.copy()
            logger.info(f"DataFrame before column mapping: {df_to_insert.columns.tolist()}")
            df_to_insert.rename(columns=column_mapping, inplace=True)
            logger.info(f"DataFrame after column mapping: {df_to_insert.columns.tolist()}")
            
            if existing_data.empty:
                logger.info("No existing data found in ap_vendorlist table. Inserting all new data.")
                # Upload data to database
                cols_to_consider = [col for col in df_to_insert.columns if col in columns_in_table]
                logger.info(f"Columns to be inserted: {cols_to_consider}")
                
                if not cols_to_consider:
                    logger.warning("No matching columns found between DataFrame and database table!")
                    logger.warning(f"DataFrame columns: {df_to_insert.columns.tolist()}")
                    logger.warning(f"Database columns: {columns_in_table}")
                    return False
                
                if df_to_insert.empty:
                    logger.warning("DataFrame is empty, nothing to insert")
                    return False
                    
                _insert_dataframe_to_sql(df_to_insert[cols_to_consider], 'ap_vendorlist', connection, logger)

                logger.info(f"Successfully inserted {len(df_to_insert)} rows into ap_vendorlist table.")
                return True
            else:
                logger.info('Data already exists in ap_vendorlist table')
                logger.info(f"Since data Already exists, only update vendor rows based on vendor_list_to_be_updated")
                # Check for duplicate values 
                cols_to_consider = ['VENDORCODE','VENDOR_NAME','vendor_address','bank_account_number',
                                    'bank_account_holder_name','PARTNER_BANK_TYPE','IBAN','SWIFT_BIC','payment_terms']
                
                # Filter columns that exist in both DataFrames FIRST
                existing_cols = [col for col in cols_to_consider if col in existing_data.columns and col in df_to_insert.columns]
                logger.info(f"Columns for duplicate check: {existing_cols}")
                
                # Only process columns that actually exist in both DataFrames
                for col in existing_cols:
                    try:
                        # Handle NaN/None values before string conversion
                        existing_data[col] = existing_data[col].fillna('').astype(str).str.strip()
                        df_to_insert[col] = df_to_insert[col].fillna('').astype(str).str.strip()
                    except Exception as e:
                        logger.warning(f"Error processing column {col} for duplicate check: {e}")
                        # Remove problematic column from comparison
                        existing_cols.remove(col)

                 # if vendor_list_to_be_updated is provided, filter unique_df accordingly
                if vendor_list_to_be_updated and len(vendor_list_to_be_updated) > 0:
                    logger.info(f"Special Processing, Update Vendor master rows with latest data from df_to_insert")
                    vendor_list_to_check = [str(vendor).strip() for vendor in vendor_list_to_be_updated]
                    existing_rows_in_db = existing_data[existing_data['VENDORCODE'].isin(vendor_list_to_check)]
                    if not existing_rows_in_db.empty:
                        logger.info(f"Existing rows in DB for specified vendors: {existing_rows_in_db.shape}")
                        logger.info(f"No of unique vendors in existing DB rows: {existing_rows_in_db['VENDORCODE'].nunique()}")
                        logger.info(f"no of rows for each vendor in existing DB rows: {existing_rows_in_db['VENDORCODE'].value_counts().to_dict()}")
                        already_present_vendor_codes = existing_rows_in_db['VENDORCODE'].unique().tolist()
                        # Delete existing rows for these vendors
                        placeholders = ','.join([f"'{code}'" for code in already_present_vendor_codes])
                        delete_query = text(f"DELETE FROM ap_vendorlist WHERE VENDORCODE IN ({placeholders})")
                        connection.execute(delete_query)
                        logger.info(f"Deleted existing rows for vendors: {already_present_vendor_codes}")

                        # Now get the new rows to insert for these vendors
                        new_vendor_rows = df_to_insert[df_to_insert['VENDORCODE'].isin(vendor_list_to_check)]
                        if not new_vendor_rows.empty:
                            logger.info(f"New rows to insert for specified vendors: {new_vendor_rows.shape}")
                            # Insert into database
                            cols_to_consider = [col for col in new_vendor_rows.columns if col in columns_in_table]
                            logger.info(f"Columns to be inserted for new vendor rows: {cols_to_consider}")
                            _insert_dataframe_to_sql(new_vendor_rows[cols_to_consider], 'ap_vendorlist', connection, logger)
                            logger.info(f"Successfully inserted {len(new_vendor_rows)} new rows for specified vendors into ap_vendorlist table.")
                            return True

                    else:
                        logger.info("No existing rows found in DB for specified vendors, follow normal insert process")

                
                if existing_cols:
                    try:
                        # Create tuples for comparison - handle potential errors
                        existing_tuples = existing_data[existing_cols].apply(tuple, axis=1)
                        new_tuples = df_to_insert[existing_cols].apply(tuple, axis=1)
                        
                        unique_df = df_to_insert[~new_tuples.isin(existing_tuples)]
                        logger.info(f"Unique rows after duplicate check: {len(unique_df)} out of {len(df_to_insert)} total rows")
                        logger.info(f"Duplicate detection based on columns: {existing_cols}")
                    except Exception as e:
                        logger.error(f"Error during duplicate detection: {e}")
                        logger.info("Falling back to inserting all rows due to duplicate check failure")
                        unique_df = df_to_insert
                else:
                    unique_df = df_to_insert  # Insert all if no matching columns
                    logger.info("No matching columns for duplicate check, inserting all rows")
   
                if unique_df.empty:
                    logger.info("No new unique rows to insert into ap_vendorlist table.")
                    return True
                else:
                    logger.info(f"Found {len(unique_df)} new unique rows to insert into ap_vendorlist table.")
                    cols_to_consider = [col for col in unique_df.columns if col in columns_in_table]
                    logger.info(f"Columns to be inserted: {cols_to_consider}")
                    
                    if not cols_to_consider:
                        logger.warning("No matching columns found between unique DataFrame and database table!")
                        return False
                        
                    if unique_df.empty:
                        logger.info("No unique rows to insert after filtering")
                        return True
                        
                    _insert_dataframe_to_sql(unique_df[cols_to_consider], 'ap_vendorlist', connection, logger)
                    logger.info(f"Successfully inserted {len(unique_df)} new rows into ap_vendorlist table.")
                    return True

    except Exception as e:
        logger.error(f"Error storing data in database: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False