import os,json,mysql.connector
import utils
from flask import  g
from databases.table_queries import create_query_credit_debit_pairs, create_query_hist_parquet_data, create_query_series_dropped_invoices, create_query_src_ap_data,create_query_ap_transaction,create_query_ap_account_document,\
    create_query_ap_transaction_score,create_query_ap_acc_doc_score,create_query_ap_transaction_flat,\
    create_query_ap_account_document_flat, create_query_duplicate_invoices, create_query_zblock_transaction_flat, create_query_zblock_account_document_flat

from databases.GL_table_queries import create_query_rpt_account_document, create_query_rpt_account_document_flat, create_query_rpt_accountdoc_score,\
    create_query_rpt_transaction_score, create_query_rpt_transaction, create_query_rpt_transaction_flat, create_query_src_gl_data
        
list_of_functions_for_ap_table_creation = [create_query_src_ap_data,create_query_ap_transaction,create_query_ap_account_document,\
    create_query_ap_transaction_score,create_query_ap_acc_doc_score,create_query_credit_debit_pairs,create_query_series_dropped_invoices] 
# removed create_query_ap_transaction_flat_view,create_query_ap_account_document_flat_view,

list_of_functions_for_gl_table_creation = [create_query_rpt_account_document, create_query_rpt_accountdoc_score,\
        create_query_rpt_transaction_score, create_query_rpt_transaction, create_query_src_gl_data ]

list_of_ap_month_tables = [create_query_duplicate_invoices, create_query_ap_transaction_flat, create_query_ap_account_document_flat]

list_of_gl_month_tables = [create_query_rpt_transaction_flat, create_query_rpt_account_document_flat]
# list_of_functions_for_table_creation = [create_query_ap_transaction_flat_view,create_query_ap_account_document_flat_view]
list_of_zblock_quarter_tables = [create_query_zblock_transaction_flat, create_query_zblock_account_document_flat]



class ShardingTables:
    
    def __init__(self):
        """
        Initializing Database variables from the DB.json
        """
        from local_database import get_database_credentials
        credentials = get_database_credentials()
        self.user = credentials["username"]
        self.password = credentials["password"]
        self.host = os.getenv("DB_HOST")
        self.port= os.getenv("DB_PORT")
        self.dbname = os.getenv("DB_NAME")
        self.ssl_ca_file = os.getenv("SSL_CA")
        self.USE_SSL_CA = os.getenv("USE_SSL_CA", "false").lower() == "true"
        self.ssl_args = {'ssl_ca': self.ssl_ca_file } if self.USE_SSL_CA else {}
        

        
    def connect_to_database(self):
        """
        Connect to Database for Fetching Data
        """
        return mysql.connector.connect(user=self.user, password=self.password,host=self.host,
                                       port=self.port,database=self.dbname,**self.ssl_args)

    def create_zblock_flat_for_each_quarter(self, quarters, module):
        """
        This function is used to dynamically create all tables and views used in entire workflow
        
        Args:
            quarters : list of quarters for the batch
            
        Returns:
            None
        
        """
        from code1.logger import capture_log_message
        try:
            capture_log_message(log_message=f'Create tables dynamically for quarters:{quarters} and module: {module}')
            if module == "ZBLOCK":
                list_of_qtr_tbs = list_of_zblock_quarter_tables
            else:
                list_of_qtr_tbs = []
            with self.connect_to_database() as connection:
                with connection.cursor() as cursor:
                    for funcn in list_of_qtr_tbs:
                            for qtr_year in quarters:
                                res = funcn(qtr_year = qtr_year)
                                if type(res)==tuple:
                                    drop_query = res[0]
                                    create_query = res[1]
                                    cursor.execute(drop_query)
                                    cursor.execute(create_query)
                                else:
                                    create_query = res
                                    cursor.execute(create_query)
        except Exception as e:
            capture_log_message(log_message=f"Error occurred while dynamically creating quarter tables, Error:{e}",
                                 current_logger=g.error_logger)

    def create_tables_for_each_audit(self, audit_id, module):
        """
        This function is used to dynamically create all tables and views used in entire workflow
        
        Args:
            audit_id : Audit id number
            
        Returns:
            None
        
        """
        from code1.logger import capture_log_message
        try:
            capture_log_message(log_message=f'Create tables dynamically for Audit:{audit_id} and module: {module}')
            if module =="AP":
                list_of_functions_for_table_creation = list_of_functions_for_ap_table_creation
            else:
                list_of_functions_for_table_creation = list_of_functions_for_gl_table_creation
            with self.connect_to_database() as connection:
                with connection.cursor() as cursor:
                    # Loop through each function in the list to get query and create corresponding tables
                    for funcn in list_of_functions_for_table_creation:
                        res = funcn(audit_id=audit_id)
                        if type(res)==tuple:
                            drop_query = res[0]
                            create_query = res[1]
                            cursor.execute(drop_query)
                            cursor.execute(create_query)
                        else:
                            create_query = res
                            cursor.execute(create_query)
        except Exception as e:
            capture_log_message(log_message=f"Error occurred while dynamically creating tables for audit {audit_id}, Error:{e}",
                                 current_logger=g.error_logger) 
    
    def delete_temp_table(self,temp_tablename, is_view = False):
        """
        This function is used to delete the temp tables 
        
        Args: temp_tablename ---> Name of temp table to be deleted
        
        Return: None
        """
        from code1.logger import capture_log_message
        try:
            capture_log_message(f"Delete temp table {temp_tablename} , is view :{is_view}")
            
            with self.connect_to_database() as connection:
                with  connection.cursor() as cursor:
                    if is_view:
                        drop_query = f"DROP VIEW IF EXISTS `{temp_tablename}`"
                    else:
                        drop_query = f"DROP TABLE IF EXISTS `{temp_tablename}`"
                    
                    capture_log_message(f"Executing query {drop_query}")
                    cursor.execute(drop_query)
                    connection.commit()
                    
                    capture_log_message(f" table {temp_tablename} deleted ")
        except Exception as e:
            capture_log_message(log_message=f"Error Occurred while deleting temp table {temp_tablename}, error {e}",
                                current_logger=g.error_logger )
            
            
    def drop_temp_tables_for_each_audit(self, workflow):
        if workflow=='ap':
            if hasattr(g,'temp_table_names'):
                for each in g.temp_table_names:
                    self.delete_temp_table(each)
        else:
            if hasattr(g,'temp_table_names'):
                for each in g.temp_table_names:
                    self.delete_temp_table(each)
    
    def drop_views_for_each_audit(self,audit_id,workflow):
        """ 
        
        This function deletes the temp tables and view after entire audit is completed
        
        Args:
            audit_id : Audit id number
            
        Returns:
            None
        
        """
        if workflow=='ap':
            transaction_viewname = "aptransactionflatview_"+str(audit_id)
            accountdoc_viewname = "apaccountdocumentsflatview_"+str(audit_id)
        else:
            transaction_viewname = ""+str(audit_id)
            accountdoc_viewname = ""+str(audit_id)
        
        self.delete_temp_table(transaction_viewname,is_view=True)
        self.delete_temp_table(accountdoc_viewname,is_view=True)
        
        
        
    def create_parquet_data_table(self, table_name):
       """
       Creates a table with the same structure as _SRC_AP_DATA but with the parquet file name.
       :param table_name: Name of the new table to be created
       """
       from code1.logger import capture_log_message
       try:
           create_table_query = create_query_hist_parquet_data(table_name)
           with self.connect_to_database() as connection:
               with connection.cursor() as cursor:
                   cursor.execute(create_table_query)
                   connection.commit()
           capture_log_message(f"{table_name} table created!")
       except Exception as e:
           capture_log_message(log_message=f"Error occurred while creating table: {table_name}, Error:{e}",
                                current_logger=g.error_logger)


    def create_flat_dupl_for_each_month(self, months, module):
        """
        This function is used to dynamically create all tables and views used in entire workflow
        
        Args:
            months : list of months for the batch
            
        Returns:
            None
        
        """
        from code1.logger import capture_log_message
        try:
            capture_log_message(log_message=f'Create tables dynamically for months:{months} and module: {module}')
            if module == "AP":
                list_of_mth_tbs = list_of_ap_month_tables
            else:
                list_of_mth_tbs = list_of_gl_month_tables
            with self.connect_to_database() as connection:
                with connection.cursor() as cursor:
                    for funcn in list_of_mth_tbs:
                            for mth_year in months:
                                res = funcn(mth_year= mth_year)
                                if type(res)==tuple:
                                    drop_query = res[0]
                                    create_query = res[1]
                                    cursor.execute(drop_query)
                                    cursor.execute(create_query)
                                else:
                                    create_query = res
                                    cursor.execute(create_query)
        except Exception as e:
            capture_log_message(log_message=f"Error occurred while dynamically creating tables for audit {g.audit_id}, Error:{e}",
                                 current_logger=g.error_logger)
        

