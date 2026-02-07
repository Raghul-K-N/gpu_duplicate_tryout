from sklearn.base import BaseEstimator
from tqdm import tqdm
from sklearn.utils.validation import check_is_fitted
from code1.logger import capture_log_message
from duplicate_invoices.model.opt_duplicate_extraction import OptimizedDuplicateDetector
from flask import g
import pandas as pd
# from duplicate_invoices.model import invoice_number_model, supplier_name_model
# from duplicate_invoices.config import config
tqdm.pandas()
import traceback

# def set_bucket(invoice_amounts, invoice_statuses, invoice_amount_abs):
#     num_paid = sum([i=='PAID' for i in invoice_statuses])
#     num_voided = sum([i=='VOIDED' for i in invoice_statuses])
#     if sum(invoice_amounts)<0 and abs(sum(invoice_amounts)) != invoice_amount_abs:
#         return 'DM Duplicates - NOT REVIEWED'
#     elif sum(invoice_amounts)>0 and abs(sum(invoice_amounts)) != invoice_amount_abs:
#         return 'IN Duplicates - NOT REVIEWED'

#     elif sum(invoice_amounts)==0 or (sum(invoice_amounts)<0 and abs(sum(invoice_amounts)) == invoice_amount_abs) or \
#         (sum(invoice_amounts)>0 and abs(sum(invoice_amounts)) == invoice_amount_abs) or \
#         (sum(invoice_amounts)==0 and num_paid == num_voided):
#         return 'IN Duplicates - REVERSED'
#     else:
#         return 'Check'


# def add_bucket_column(dupl, column):
#     t = dupl.groupby(column)['INVOICE_AMOUNT', 'INVOICE_STATUS', 'INVOICE_AMOUNT_ABS'].agg(lambda x: list(x))

#     t['INVOICE_CATEGORY'] = t.apply(lambda x: set_bucket(x['INVOICE_AMOUNT'], x['INVOICE_STATUS'], x['INVOICE_AMOUNT_ABS'][0]), axis=1)
#     to_join = t[['INVOICE_CATEGORY']]
#     dupl = dupl.join(to_join, on=column, how='left')

#     return dupl


# def get_combined_set(current_pk, ml_pk_to_duplicate_id, ml_duplicate_id_to_pks_list, \
#     rule_pk_to_duplicate_id, rule_duplicate_id_to_pks_list):
#     ml_group = []
#     rule_group = []
#     if current_pk in ml_pk_to_duplicate_id:
#         ml_group = ml_duplicate_id_to_pks_list[ml_pk_to_duplicate_id[current_pk]]
#     if current_pk in rule_pk_to_duplicate_id:
#         rule_group = rule_duplicate_id_to_pks_list[rule_pk_to_duplicate_id[current_pk]]
#     combined_set = set(ml_group + rule_group)

#     return combined_set


# # Alternate function for combined_mapping_dict , new function is more optimized and easy to debug

# # import networkx as nx
# # from itertools import combinations
# # def combined_mapping_graph(output_ml, output_rule_based):
# #    # Preallocate edges
# #    edges = []
  
# #    # Add edges for ML-based duplicate groups
# #    for _, group in output_ml.groupby('DUPLICATE_ID_ML'):
# #        pks = group['PrimaryKeySimple'].tolist()
# #        edges.extend(combinations(pks, 2))  # Create all pairwise edges
  
# #    # Add edges for Rule-based duplicate groups
# #    for _, group in output_rule_based.groupby('DUPLICATE_ID_RULE_BASED'):
# #        pks = group['PrimaryKeySimple'].tolist()
# #        edges.extend(combinations(pks, 2))  # Create all pairwise edges
  
# #    # Build the graph in one step
# #    G = nx.Graph()
# #    G.add_edges_from(edges)
  
# #    # Assign DUPLICATE_IDs using connected components
# #    pk_to_duplicate_id = {
# #        pk: duplicate_id
# #        for duplicate_id, component in enumerate(nx.connected_components(G))
# #        for pk in component
# #    }
# #    return pk_to_duplicate_id


# def combined_mapping_dict(output_ml, output_rule_based):
#     pk_rule = set(output_rule_based['PrimaryKeySimple'].to_list())
#     pk_ml = set(output_ml['PrimaryKeySimple'].to_list())
#     all_duplicate_pks = pk_rule.union(pk_ml)

#     ml_pk_to_duplicate_id = output_ml[['DUPLICATE_ID_ML', 'PrimaryKeySimple']].set_index('PrimaryKeySimple').to_dict()['DUPLICATE_ID_ML']
#     ml_duplicate_id_to_pks_list = output_ml[['DUPLICATE_ID_ML', 'PrimaryKeySimple']].groupby('DUPLICATE_ID_ML').agg(lambda x: list(x)).to_dict()['PrimaryKeySimple']

#     rule_pk_to_duplicate_id = output_rule_based[['DUPLICATE_ID_RULE_BASED', 'PrimaryKeySimple']].set_index('PrimaryKeySimple').to_dict()['DUPLICATE_ID_RULE_BASED']
#     rule_duplicate_id_to_pks_list = output_rule_based[['DUPLICATE_ID_RULE_BASED', 'PrimaryKeySimple']].groupby('DUPLICATE_ID_RULE_BASED').agg(lambda x: list(x)).to_dict()['PrimaryKeySimple']    

#     all_duplicate_pks = list(all_duplicate_pks)
#     duplicate_id = 0
#     pk_to_duplicate_id = dict()

#     while len(all_duplicate_pks):
#         current_pk = all_duplicate_pks[0]
#         combined_set = get_combined_set(current_pk, ml_pk_to_duplicate_id, ml_duplicate_id_to_pks_list, \
#         rule_pk_to_duplicate_id, rule_duplicate_id_to_pks_list)

#         final_combined_set = set()

#         for pk in combined_set:
#             final_combined_set = final_combined_set.union(get_combined_set(pk, ml_pk_to_duplicate_id, ml_duplicate_id_to_pks_list, \
#             rule_pk_to_duplicate_id, rule_duplicate_id_to_pks_list))

#         for pk in final_combined_set:
#             pk_to_duplicate_id[pk] = duplicate_id
#             if pk in all_duplicate_pks:
#                 all_duplicate_pks.remove(pk)

#         duplicate_id += 1

#     return pk_to_duplicate_id



class DuplicateExtract(BaseEstimator):
    """
    Pipeline BaseEstimator wrapper for duplicate detection
    This class handles the sklearn pipeline integration
    """
    

    def fit(self, X, y=None):
        """Fit the model - required for sklearn compatibility"""
        self.is_fitted_ = True 
        return self

    def predict(self, df, y=None):
        """
        Main prediction method for pipeline integration
        
        Args:
            df: Input DataFrame to check for duplicates
            y: Not used, kept for sklearn compatibility
            
        Returns:
            tuple: (duplicates_df, None, None) for backward compatibility
        """
        # check_is_fitted(self, ["is_fitted_"])
        
        try:
            # Create detector instance
            detector = OptimizedDuplicateDetector(df)
            
            # Run duplicate detection
            duplicates_df = detector.detect_duplicates()
            
            if duplicates_df.empty:
                capture_log_message('No duplicates found across all scenarios')
                return pd.DataFrame()
            

            capture_log_message(f'Duplicate detection completed. Found {len(duplicates_df)} duplicate records in {duplicates_df["DUPLICATE_ID"].nunique()} groups')
            
            # Return in the expected format (duplicates, output_ml, output_rule_based)
            # For backward compatibility, we return None for the latter two
            return duplicates_df
            
        except Exception as e:
            capture_log_message(current_logger=g.error_logger, log_message=f'Error in duplicate detection: {str(e)}')
            capture_log_message(current_logger=g.error_logger,
                            log_message= str(traceback.format_exc()), store_in_db=False)
            return None






        # if config.SUPPLIER_SIMILARITY_CHECK:
        #     df = supplier_name_model.duplicate_suppliername_similar(df, config.SUPPLIER_NAME_COLUMN, \
        #         config.SUPPLIER_MODEL_GROUPING_COLUMNS, 'SUPPLIER_NAME_ML', exact_matching=False)
        #     df = df[df['DUPLICATE_ID_SUPPLIER_NAME_ML'].notnull()]
        #     grouping_columns = config.INVOICE_MODEL_GROUPING_COLUMNS
        # else:
        #     grouping_columns = config.GROUPING_COLUMNS
        
        # output_rule_based = invoice_number_model.duplicate_invoicenumber_similar(df.copy(deep=True), \
        #     'INVOICE_NUMBER_FORMAT', grouping_columns, \
        #     'RULE_BASED', exact_matching=config.EXACT_MATCHING, model='RULE_BASED')
        # capture_log_message('Rule based Calculation Completed')
        # if len(output_rule_based) >0:
        #     output_rule_based = output_rule_based[output_rule_based['DUPLICATE_FLAG_RULE_BASED']]
        #     # output_rule_based.to_csv('output_rule_based_amc.csv')
        

            
        #     output_ml = invoice_number_model.duplicate_invoicenumber_similar(df.copy(deep=True), \
        #         'INVOICE_NUMBER_FORMAT', grouping_columns, \
        #         'ML', exact_matching=config.EXACT_MATCHING, model='ML')
        #     output_ml = output_ml[output_ml['DUPLICATE_FLAG_ML']]
        #     # output_ml.to_csv('output_ml_amc.csv')
        #     capture_log_message('ML based calculation completed')

        #     pk_to_duplicate_id = combined_mapping_dict(output_ml, output_rule_based)
        #     df['DUPLICATE_ID'] = df['PrimaryKeySimple'].map(pk_to_duplicate_id)
        #     duplicates = df[df['DUPLICATE_ID'].notnull()]
        #     if duplicates.empty:
        #         return duplicates , output_ml,output_rule_based
        #     duplicates['DUPLICATE_ID'] = duplicates['DUPLICATE_ID'].astype(int)

        #     output_ml['DUPLICATE_RISK_SCORE_ML'] = output_ml['DUPLICATE_RISK_SCORE_ML'].apply(lambda x: sum(x)/len(x))
            
        #     output_rule_based['DUPLICATE_RISK_SCORE_RULE_BASED'] = output_rule_based['DUPLICATE_RISK_SCORE_RULE_BASED'].apply(lambda x: sum(x) / len(x)  if isinstance(x, list) else x)
            
            
        #     pk_to_duplicate_risk_score_rule = output_rule_based[['PrimaryKeySimple', 'DUPLICATE_RISK_SCORE_RULE_BASED']]\
        #                                 .set_index('PrimaryKeySimple').to_dict()['DUPLICATE_RISK_SCORE_RULE_BASED']

        #     pk_to_duplicate_risk_score = output_ml[['PrimaryKeySimple', 'DUPLICATE_RISK_SCORE_ML']]\
        #                                 .set_index('PrimaryKeySimple').to_dict()['DUPLICATE_RISK_SCORE_ML']
            
        #     # The combine_first() method in pandas is used to combine two DataFrames or Series objects by
        #     # filling in missing values in one with values from the other.
        #     duplicates['DUPLICATE_RISK_SCORE'] = duplicates['PrimaryKeySimple'].map(pk_to_duplicate_risk_score_rule)
        #     duplicates['DUPLICATE_RISK_SCORE'] = duplicates['PrimaryKeySimple'].map(pk_to_duplicate_risk_score).combine_first(duplicates['DUPLICATE_RISK_SCORE'])
            
        #     # duplicates = duplicates[~(duplicates['DUPLICATE_RISK_SCORE'].isna())] 

        #     # duplicates['COMPANY'] = "AM Calvert"
        #     if config.MODE == 'CCS':
        #         duplicates = add_bucket_column(duplicates, 'DUPLICATE_ID')
        #         duplicates.rename(columns = {'INVOICE_ENTERED_DATE':'POSTED_DATE',
        #                                 'ENTERED_BY': 'POSTED_BY',
        #                                 'CHECK_DATE': 'PAYMENT_DATE'}, inplace = True)
        #         # duplicates['COMPANY'] = "CCS"


        #     duplicates['NO_OF_DUPLICATES'] = duplicates.groupby('DUPLICATE_ID')['DUPLICATE_ID'].transform('count')

        #     return duplicates, output_ml, output_rule_based

        # else:
        #     # from code1.logger import logger
        #     capture_log_message(log_message='No rule based duplicates found')
    