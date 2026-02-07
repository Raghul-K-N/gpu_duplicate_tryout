from itertools import combinations
from functools import partial
from joblib import Parallel, delayed
from tqdm import tqdm
import pandas as pd
import os
from duplicate_invoices.model import duplicate_extract_helper as dupl_helper
from pandarallel import pandarallel
from duplicate_invoices.config.config import POSTED_DATE_THRESHOLD

# GPU Configuration - Import with fallback
try:
    from duplicate_invoices.config.gpu_config import USE_GPU, GPU_CONFIG
    from duplicate_invoices.gpu.tf_backend import TFDuplicateAccelerator, is_gpu_available
    GPU_AVAILABLE = USE_GPU and is_gpu_available()
except ImportError:
    GPU_AVAILABLE = False
    USE_GPU = False

# Initialize pandarallel for parallel processing
NO_OF_WORKERS = os.cpu_count() - 1 if os.cpu_count() > 1 else 1
pandarallel.initialize(progress_bar=True, nb_workers=NO_OF_WORKERS)

class OptimizedDuplicateDetector:
    """
    Optimized duplicate detection logic - standalone class
    Can be used independently for testing or within the pipeline
    
    GPU Acceleration:
        Set USE_GPU=True in gpu_config.py to enable TensorFlow GPU acceleration.
        GPU is automatically used for batch similarity computation when available.
    """
    
    def __init__(self, df, use_gpu: bool = None):
        self.scenarios_df = dupl_helper.get_scenario_data_for_duplicates()
        
        # State management
        self.df = df.copy()
        self.duplicate_pairs = {}  # Global duplicate pairs across all scenarios
        self.processed_non_duplicates = set()  # Global set of confirmed non-duplicates
        
        # GPU Acceleration - Initialize if available and enabled
        self.use_gpu = use_gpu if use_gpu is not None else GPU_AVAILABLE
        self.gpu_accelerator = None
        if self.use_gpu and GPU_AVAILABLE:
            try:
                self.gpu_accelerator = TFDuplicateAccelerator(
                    batch_size=GPU_CONFIG.get('batch_size', 10000)
                )
                from code1.logger import capture_log_message
                capture_log_message("GPU acceleration enabled for duplicate detection")
            except Exception as e:
                from code1.logger import capture_log_message
                capture_log_message(f"GPU initialization failed, falling back to CPU: {e}")
                self.use_gpu = False
        
        
    def _create_comparison_value(self, row, columns):
        """Create comparison value for similarity checking"""
        if not columns or columns == '':
            columns = 'POSTING_DATE'
        
        column_list = [col.strip() for col in columns.split(',')]
        values = []
        for col in column_list:
            if col in row.index:
                values.append(str(row[col]))
            else:
                values.append('')
        
        return f"{'-'.join(values)}"
    

    def _similarity_function(self, source_value, dest_value):
        """Wrapper for existing similarity function"""
        return dupl_helper.is_invoice_similar(source_value, dest_value)
    
    
    def _check_row(self, row):
        """
        Check a single row for duplicates
        
        Args:
            row: Series containing pair information
            
        Returns:
            tuple: (pair_key, is_duplicate, score)
        """
        pair_key = row['pair_key']
        source_value = row['source_value']
        dest_value = row['dest_value']
        is_current_data_i = row['is_current_data_i']
        is_current_data_j = row['is_current_data_j']
        pk_i = row['pk_i']
        pk_j = row['pk_j']
        
        
        # Skip if already processed
        
        # Update: Calculate pairs even it was computed previously, duplicate pairs can be dropped at final step

        # if pair_key in self.duplicate_pairs or pair_key in self.processed_non_duplicates:
        #     return None
        
        # Check current data constraint
        if not (is_current_data_i or is_current_data_j):
            return None
        
        # Check similarity
        is_duplicate, score = self._similarity_function(source_value, dest_value)
        
        return (is_duplicate , pair_key, score, pk_i,pk_j)
    

    def _process_group_pairs(self, group_indices, similarity_columns, process_parallely=False):
        """
        Function that processes a group using indices.
        If  the number of invoices is large, possible number of combinations is also large.
        Hence, we use parallel processing to speed up the pairwise comparison. Else its a normal processing for smaller groups.
        
        
        Args:
            group_indices: List of DataFrame indices for the group
            similarity_columns: Columns to use for similarity comparison
            process_parallely: If True, use parallel processing for large groups, 
            
        Returns:
            tuple: (local_duplicates_dict, local_non_duplicates_set)
        """
        # Slice the main DataFrame using indices (memory efficient)
        all_rows = []
        group_df = self.df.loc[group_indices].reset_index(drop=True)
        group_df['comparison_value'] = group_df.apply( lambda row: self._create_comparison_value(row, similarity_columns), axis=1 )

        local_duplicates = {}
        local_non_duplicates = set()
        
        if len(group_df) < 2:
            return local_duplicates,local_non_duplicates
        
        # Process all pairs within the group
        for i, j in combinations(group_df.index, 2):
            row_i = group_df.iloc[i]
            row_j = group_df.iloc[j]
            
            pk_i = row_i['PrimaryKeySimple']
            pk_j = row_j['PrimaryKeySimple']
            
            # Create consistent pair key (smaller first)
            pair_key = tuple(sorted([pk_i, pk_j]))
            
            # Skip if already processed (check against global state)

            # Update: Calculate pairs even it was computed previously, duplicate pairs can be dropped at final step
            # if pair_key in self.duplicate_pairs:
            #     continue
            
            is_current_data_i = row_i.get('is_current_data', True)
            is_current_data_j = row_j.get('is_current_data', True)
            
            # Check current data constraint
            
            if not (is_current_data_i or is_current_data_j):
                continue
            
            # Create comparison values
            source_value = group_df.iloc[i]['comparison_value']
            dest_value = group_df.iloc[j]['comparison_value']
            
            
            
            # If process_parallely is set True, store row wise info only
            if process_parallely:
                all_rows.append({
                'pair_key': pair_key,
                'source_value': source_value,
                'dest_value': dest_value,
                'pk_i': pk_i,
                'pk_j': pk_j,
                'is_current_data_i': is_current_data_i,
                'is_current_data_j': is_current_data_j
            })
            else:  
                # Check similarity
                is_duplicate, score = self._similarity_function(source_value, dest_value)
                
                if is_duplicate:
                    local_duplicates[pair_key] = {
                        'score': score,
                        'source_pk': pk_i,
                        'dest_pk': pk_j
                    }
                else:
                    local_non_duplicates.add(pair_key)
                    
        if process_parallely:
            # Store all rows in a DataFrame for parallel processing
            pairs_df = pd.DataFrame(all_rows)
            
            # Use pandarallel to process all rows in parallel
            if pairs_df.shape[0]!=0:
                results = pairs_df.parallel_apply(lambda row:self._check_row(row),axis=1) # type: ignore
                
                for result in results.dropna():
                    is_duplicate, pair_key, score, pk_i, pk_j = result
                    if is_duplicate:
                        local_duplicates[pair_key] = {
                            'score': score,
                            'source_pk': pk_i,
                            'dest_pk': pk_j
                        }
                    else:
                        # If not a duplicate, we could track non-duplicates if needed
                        local_non_duplicates.add(pair_key)
            
        return local_duplicates,local_non_duplicates

    def _process_special_case(self, group_indices, similarity_columns):
        # Slice the main DataFrame using indices (memory efficient)
        all_rows = []
        group_df = self.df.loc[group_indices].reset_index(drop=True)
        group_df['comparison_value'] = group_df.apply( lambda row: self._create_comparison_value(row, similarity_columns), axis=1 )

        local_duplicates = {}
        local_non_duplicates = set()
        
        if len(group_df) < 2:
            return local_duplicates,local_non_duplicates
        
        # Process all pairs within the group
        for i, j in combinations(group_df.index, 2):
            row_i = group_df.iloc[i]
            row_j = group_df.iloc[j]
            
            pk_i = row_i['PrimaryKeySimple']
            pk_j = row_j['PrimaryKeySimple']
            
            # Create consistent pair key (smaller first)
            # print("sonsistent")
            pair_key = tuple(sorted([pk_i, pk_j]))
            
            # Skip if already processed (check against global state)
            # print("skip")

            # check and continue to next pair if both invoice numbers are pure numbers
            source_invoice_number = group_df.iloc[i]['INVOICE_NUMBER_FORMAT']
            dest_invoice_number = group_df.iloc[j]['INVOICE_NUMBER_FORMAT']
            if source_invoice_number.isdigit() and dest_invoice_number.isdigit():
                continue
            
            # Update: Calculate pairs even it was computed previously, duplicate pairs can be dropped at final step
            # if pair_key in self.duplicate_pairs:
            #     continue
            
            is_current_data_i = row_i.get('is_current_data', True)
            is_current_data_j = row_j.get('is_current_data', True)
            
            # Check current data constraint
            # print("current")
            if not (is_current_data_i or is_current_data_j):
                continue
            
            # Create comparison values
            # print(f"i: {i} adn {j} and create: {group_df}")
            source_value = group_df.iloc[i]['comparison_value']
            dest_value = group_df.iloc[j]['comparison_value']

            # Check similarity
            # print(f"source: {source_value} and dest: {dest_value}")
            # is_duplicate, score = dupl_helper._posted_date_similarity(source_value, dest_value, threshold=POSTED_DATE_THRESHOLD)
            # is_duplicate, score = True, 85.0
            # is_duplicate, score = (False, 0) if dupl_helper.is_sequential_series(str(source_invoice_number),str(dest_invoice_number)) else (True, 85.0)
            # if is_duplicate:
            is_duplicate, score = dupl_helper._posted_date_similarity(source_value, dest_value, threshold=POSTED_DATE_THRESHOLD)

            if is_duplicate:
                local_duplicates[pair_key] = {
                    'score': score,
                    'source_pk': pk_i,
                    'dest_pk': pk_j
                }
            else:
                local_non_duplicates.add(pair_key)
            
        return local_duplicates,local_non_duplicates

    def _process_scenario_optimized(self, scenario_info):
        """Process a single scenario with optimized parallel processing"""
        scenario_id = scenario_info['SCENARIO_ID']
        group_by_fields = [col.strip() for col in scenario_info['GROUP_BY_FIELDS'].split(',')]
        group_by_fields.append('REGION')
        similarity_columns = scenario_info['SIMILARITY_CHECK_COLUMNS']
        
        from code1.logger import capture_log_message
        capture_log_message(f"Processing Scenario {scenario_id}: {scenario_info['SCENARIO_NAME']}")

        group_lengths = []
        group_indices_list_small = [] # Store all indices when group size is less than 500
        group_indices_list_large = [] # Store indices for groups with size >= 500
        size_threshold = 500
        
        # Apply groupby on whole data based on groupby columns, and store indices of grouped rows
        for _, group_df in self.df.groupby(group_by_fields):
            if len(group_df) >= 2:
                if len(group_df) >= size_threshold:
                    # For large groups, store indices separately
                    group_indices_list_large.append(group_df.index.tolist())
                else:
                    group_indices_list_small.append(group_df.index.tolist())
                
                group_lengths.append(len(group_df))
                
        # Bucket group lengths and show summary 
        bins = [0,100,200,300,400,500,600,700,800,900,1000,float('inf')]
        bin_labels = [f"{bins[i]}-{bins[i+1]}" for i in range(len(bins)-1)]
        group_length_binned = pd.cut(pd.Series(group_lengths),bins=bins, labels=bin_labels)
        capture_log_message(f"Group lengths for scenario: {scenario_id}, len:{len(group_lengths)}")
        capture_log_message(f"Number of small groups: {len(group_indices_list_small)}")
        capture_log_message(f"Number of large groups: {len(group_indices_list_large)}")
        capture_log_message(log_message=f"{group_length_binned.value_counts().sort_index()}",store_in_db=False)
        
        if group_indices_list_small:
            main_df = pd.DataFrame({'group_indices':group_indices_list_small})
            capture_log_message(f"No of Groups that are small: {main_df.shape}")
            
            # If no columns are present in the list, its a special case
            if similarity_columns: 
                results = main_df.parallel_apply(lambda x: self._process_group_pairs(x['group_indices'],
                                                                                    similarity_columns),
                                                                                    axis=1) # type: ignore
            else: # Special case where no columns are present
                results = main_df.parallel_apply(lambda x: self._process_special_case(x['group_indices'],
                                                                                    similarity_columns), 
                                                                                    axis=1) # type: ignore
            
            for local_duplicates,local_non_duplicates in results:
                for pair_key, pair_info in local_duplicates.items():
                    pair_key =  (pair_key,scenario_id) # store pair info with scenario id to avoid conflicts across scenarios
                    if pair_key not in self.duplicate_pairs:
                        pair_info['SCENARIO_ID'] = scenario_id
                        self.duplicate_pairs[pair_key] = pair_info
                    
                self.processed_non_duplicates.update(local_non_duplicates)
            
        # Not running large Groups as of Now

        
        # if group_indices_list_large:
        #     main_df_large = pd.DataFrame({'group_indices':group_indices_list_large})
        #     capture_log_message(f"No of Groups that are large: {main_df_large.shape}")
            
        #     if similarity_columns:
        #         results_large = main_df_large.apply(lambda x:self._process_group_pairs(group_indices=x['group_indices'],
        #                                                                         similarity_columns=similarity_columns,
        #                                                                         process_parallely=True),  # process parallely within a group
        #                                                                         axis=1)
        #     else: # Special case where no columns are present
        #         results_large = main_df_large.parallel_apply(lambda x: self._process_special_case(x['group_indices'],
        #                                                                             similarity_columns), 
        #                                                                             axis=1) # type: ignore
            
        #     for local_duplicates, local_non_duplicates in results_large:
        #         for pair_key, pair_info in local_duplicates.items():
        #             if pair_key not in self.duplicate_pairs:
        #                 pair_info['SCENARIO_ID'] = scenario_id
        #                 self.duplicate_pairs[pair_key] = pair_info
                        
        #         self.processed_non_duplicates.update(local_non_duplicates)
            
            
        capture_log_message(f"Length of processed duplicate pairs:{len(self.duplicate_pairs)}")
        capture_log_message(f"Length of processed non-duplicates:{len(self.processed_non_duplicates)}")
            
    


    def _create_final_results_optimized(self):
        """Create final results using graph-based approach - single pass"""
        if not self.duplicate_pairs:
            return pd.DataFrame()
        
        pairs_df = pd.DataFrame.from_dict(self.duplicate_pairs, orient='index')
        pairs_df.reset_index(inplace=True, drop=True)
        
        # Process each scenario to create graph-based groups
        scenario_results = []
        from code1.logger import capture_log_message
        for scenario_id in pairs_df['SCENARIO_ID'].unique():
            scenario_pairs = pairs_df[pairs_df['SCENARIO_ID'] == scenario_id][
                ['source_pk', 'dest_pk', 'score']
            ]
            
            if scenario_pairs.empty:
                continue
            
            scenario_df = dupl_helper.create_graph_based_groups(scenario_pairs, scenario_id)
            if not scenario_df.empty:
                scenario_results.append(scenario_df)
                capture_log_message(f"Scenario {scenario_id}: {len(scenario_df)} records in {scenario_df['group_uuid'].nunique()} groups")
        
        if not scenario_results:
            capture_log_message("No final duplicates after processing")
            return pd.DataFrame()
        
        # Combine all scenarios
        all_groups_df = pd.concat(scenario_results, ignore_index=True)
        
        # Assign continuous DUPLICATE_IDs using factorize
        all_groups_df['DUPLICATE_ID'] = pd.factorize(all_groups_df['group_uuid'])[0] + 1
        all_groups_df = all_groups_df.drop(columns=['group_uuid'])
        
        # Single merge back to original data
        result_df = all_groups_df.merge(
            self.df,
            on='PrimaryKeySimple',
            how='left'
        )
        
        # Add metadata
        result_df['NO_OF_DUPLICATES'] = result_df.groupby(['DUPLICATE_ID', 'SCENARIO_ID'])['DUPLICATE_ID'].transform('count')
        
        return result_df
    

    def detect_duplicates(self):
        """
        Main duplicate detection method - can be called independently
        
        Args:
            df: Input DataFrame to check for duplicates
            
        Returns:
            pd.DataFrame: DataFrame with duplicate records and metadata
        """
        # Store DataFrame in class state
        
        # Reset tracking variables
        self.duplicate_pairs = {}
        # self.processed_non_duplicates = set()
        from code1.logger import capture_log_message
        capture_log_message(f"Processing {len(self.df)} records across scenarios...")
        
        # Process each scenario in priority order
        active_scenarios = self.scenarios_df[self.scenarios_df['STATUS'] == 1].sort_values('SCENARIO_ID')
        
        for _, scenario_info in active_scenarios.iterrows():
            self._process_scenario_optimized(scenario_info)
        
        # Create final results using optimized approach
        duplicates_df = self._create_final_results_optimized()
        
        if duplicates_df.empty:
            capture_log_message("No duplicates found across all scenarios")
            return pd.DataFrame()
        
        # capture_log_message summary
        capture_log_message(f"\n=== FINAL RESULTS ===")
        capture_log_message(f"Total duplicate records: {len(duplicates_df)}")
        capture_log_message(f"Total duplicate groups: {duplicates_df.groupby(['DUPLICATE_ID', 'SCENARIO_ID']).ngroups}")
        
        # Summary by scenario
        scenario_summary = duplicates_df.groupby('SCENARIO_ID').agg({
            'PrimaryKeySimple': 'count',
            'DUPLICATE_ID': 'nunique'
        }).rename(columns={'PrimaryKeySimple': 'Records', 'DUPLICATE_ID': 'Groups'})
        capture_log_message("\nDuplicate summary by scenario:")
        capture_log_message(f"\n{scenario_summary}")

        
        score_bins = [60, 70, 80, 90, 100]
        score_labels = ["60-70", "70-80", "80-90", "90-100"]
        # Assign buckets to each score
        duplicates_df["ScoreBucket"] = pd.cut(
            duplicates_df["DUPLICATE_RISK_SCORE"], 
            bins=score_bins, 
            labels=score_labels, 
            right=True, 
            include_lowest=True
        )

        # Count distribution per SCENARIO_ID
        score_distribution = (
            duplicates_df.groupby(["SCENARIO_ID", "ScoreBucket"])
            .size()
            .unstack(fill_value=0)
        )

        capture_log_message("\nDuplicate summary by Score bucket:")
        capture_log_message(f"\n{score_distribution}")

        
        return duplicates_df

