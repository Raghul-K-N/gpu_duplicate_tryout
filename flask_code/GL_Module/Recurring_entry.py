import pandas as pd

class RecurringEntriesDetector:
    """
    A class to detect recurring accounting entries in transaction data.
    
    This detector analyzes transaction data against predefined recurring entry rules
    to identify whether expected recurring entries are present in each time period.
    It supports both monthly and quarterly frequency detection.
    """
    
    def __init__(
        self,
        transactions_df: pd.DataFrame,
        recurring_df: pd.DataFrame,
        chart_accounts_df: pd.DataFrame = None
    ):
        """
        Initialize the RecurringEntriesDetector with transaction data and recurring rules.
        
        :param transactions_df: DataFrame with columns:
            ['POSTED_DATE', 'account_doc_id', 'ACCOUNT_CODE', 'DEBIT_CREDIT_INDICATOR', 'amount', 'transaction_text']
        :param recurring_df: DataFrame of recurring rules with columns:
            ['entry_id', 'frequency', 'credit_accounts', 'debit_accounts',
             'keywords', 'amount', 'amount_deviation', 'min_amount', 'max_amount']
        :param chart_accounts_df: Optional DataFrame containing chart of accounts information
        """
        # Copy and normalize dates
        self.chart_accounts_df = chart_accounts_df
        self.transactions = transactions_df.copy()
        self.transactions['POSTED_DATE'] = pd.to_datetime(
            self.transactions.get('POSTED_DATE', self.transactions.index)
        )

        # Add period columns for grouping transactions by time periods
        self.transactions['month_year'] = self.transactions['POSTED_DATE'].dt.to_period('M').astype(str)
        self.transactions['quarter_year'] = self.transactions['POSTED_DATE'].dt.to_period('Q').astype(str)

        # Pre-store rules for recurring entry detection
        self.recurring_entries = recurring_df.copy()

        # Pre-compute all periods to ensure complete coverage
        self.all_months = sorted(self.transactions['month_year'].unique())
        self.all_quarters = sorted(self.transactions['quarter_year'].unique())

    def detect(self) -> pd.DataFrame:
        """
        Detects recurring entries across time periods.
        
        Processes each recurring entry rule against all relevant time periods
        to determine presence, amounts, and document references.
        
        Returns DataFrame: ['entry_id', 'period', 'is_present', 'recorded_amount', 'account_doc_number']
        """
        records = []
        
        # Process each recurring entry rule
        for _, entry in self.recurring_entries.iterrows():
            freq = entry['frequency'].lower()
            
            # Select appropriate time periods based on frequency
            periods = self.all_months if freq == 'monthly' else self.all_quarters
            period_col = 'month_year' if freq == 'monthly' else 'quarter_year'

            # Check each period for this recurring entry
            for period in periods:
                # Derive year and month based on frequency
                if freq == 'monthly':
                    year, month = period.split('-')
                    month = int(month)
                else:  # 'quarterly'
                    # Example period: '2024Q1'
                    year = int(period[:4])
                    quarter = int(period[-1])
                    # You can set `month` as the starting month of the quarter
                    month = (quarter - 1) * 3 + 1
                # Slice raw transactions for the period
                present, rec_amount, doc_id = self._process_recurring_entry(period_col=period_col, period=period, entry=entry)

                records.append({
                    'entry_id': entry['entry_id'],
                    'year': year,
                    'month': month,
                    'is_present': present,
                    'recorded_amount': rec_amount,
                    'account_doc_number': doc_id
                })
        
        return pd.DataFrame(records)


    def _process_recurring_entry(self, period_col, period, entry):
        """
        Process a single recurring entry for a specific time period.
        
        Filters transactions for the given period, preprocesses them,
        and checks for matches against the recurring entry criteria.
        
        :param period_col: Column name for the time period ('month_year' or 'quarter_year')
        :param period: Specific period value to filter by
        :param entry: Recurring entry rule to match against
        :return: Tuple of (is_present, recorded_amount, document_id)
        """
        # Filter transactions for the specific period
        df_period = self.transactions[self.transactions[period_col] == period]

        # Preprocess only this period's transactions
        df_preprocessed = self._preprocess_transactions(df_period)

        # Filter by account pair (credit and debit accounts)
        df_match = df_preprocessed[
            (df_preprocessed['CREDIT_ACCOUNT'] == entry['credit_accounts']) &
            (df_preprocessed['DEBIT_ACCOUNT']  == entry['debit_accounts'])
        ]

        # Initialize return values
        present, rec_amount, doc_id = False, None, None
        
        # Check each matching document for amount and keyword criteria
        for _, doc in df_match.iterrows():
            # Check if amount matches the recurring entry criteria
            if not self._match_amount(doc['AMOUNT'], entry['amount'],
                                        entry['amount_deviation'],
                                        entry['min_amount'], entry['max_amount']):
                continue
            
            # Check if keywords match
            keywords = [kw.strip().lower()
                        for kw in (entry.get('keywords') or '').split(',') if kw.strip()]
            if not self._match_keywords(doc['TRANSACTION_DESCRIPTION'], keywords):
                continue
            
            # If we reach here, we found a match
            present = True
            rec_amount = doc['AMOUNT']
            doc_id = doc['ACCOUNTDOCID']
            return present, rec_amount, doc_id
        
        return present, rec_amount, doc_id


    def _preprocess_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess transactions for a given time period.
        
        For a subset of transactions, aggregate per document:
        - Create CREDIT_ACCOUNT/DEBIT_ACCOUNT via DEBIT_CREDIT_INDICATOR
        - Sum amounts, pick first non-empty text, carry POSTED_DATE
        
        :param df: DataFrame containing transactions for a specific period
        :return: Preprocessed DataFrame with aggregated document-level data
        """
        df_work = df.copy()
        
        # Determine credit and debit accounts based on amount indicators
        df_work['CREDIT_ACCOUNT'] = df_work.apply(
            lambda x: x['ACCOUNT_CODE'] if x['CREDIT_AMOUNT']>0 else None,
            axis=1
        )
        df_work['DEBIT_ACCOUNT'] = df_work.apply(
            lambda x: x['ACCOUNT_CODE'] if x['DEBIT_AMOUNT']>0 else None,
            axis=1
        )
        
        # Aggregate transactions by document ID
        agg = df_work.groupby('ACCOUNTDOCID').agg({
            'CREDIT_ACCOUNT': lambda v: list(v.dropna().unique()),
            'DEBIT_ACCOUNT' : lambda v: list(v.dropna().unique()),
            'AMOUNT'        : 'sum',
            'TRANSACTION_DESCRIPTION': lambda v: next((t for t in v if pd.notna(t) and t), ''),
            'POSTED_DATE'   : 'first'
        }).reset_index()
        
        # Filter to only include documents with single credit & debit accounts
        agg = agg[(agg['CREDIT_ACCOUNT'].apply(len) == 1) & (agg['DEBIT_ACCOUNT'].apply(len) == 1)]
        
        # Convert account lists to single values
        agg['CREDIT_ACCOUNT'] = agg['CREDIT_ACCOUNT'].apply(lambda a: a[0])
        agg['DEBIT_ACCOUNT']  = agg['DEBIT_ACCOUNT'] .apply(lambda a: a[0])
        
        return agg

    def _match_amount(self, transaction_amount: float, entry_amount: float,
                      deviation: float, min_amt: float, max_amt: float) -> bool:
        """
        Check if a transaction amount matches the recurring entry amount criteria.
        
        Supports three types of matching:
        1. Exact amount match
        2. Deviation-based match (percentage tolerance)
        3. Range-based match (min/max bounds)
        
        :param transaction_amount: Amount from the transaction
        :param entry_amount: Expected amount from recurring entry rule
        :param deviation: Allowed percentage deviation from expected amount
        :param min_amt: Minimum acceptable amount
        :param max_amt: Maximum acceptable amount
        :return: True if amount matches criteria, False otherwise
        """
        # Exact match
        if entry_amount is not None and transaction_amount == entry_amount:
            return True
        
        # Deviation-based match
        if entry_amount is not None and deviation is not None:
            deviation = abs((float(deviation)/100) * entry_amount)
            return abs(transaction_amount - entry_amount) <= deviation
        
        # Range-based match
        if min_amt is not None and max_amt is not None:
            return min_amt <= transaction_amount <= max_amt
        
        # No constraints provided condition
        return False

    def _match_keywords(self, text: str, keywords: list) -> bool:
        """
        Check if transaction description contains any of the specified keywords.
        
        Performs case-insensitive keyword matching in transaction descriptions.
        
        :param text: Transaction description text to search in
        :param keywords: List of keywords to search for
        :return: True if any keyword is found or no keywords specified, False otherwise
        """
        # If no keywords specified, consider it a match
        if not keywords:
            return True
        
        # Convert text to lowercase for case-insensitive matching
        txt = (text or '').lower()
        
        # Check if any keyword is present in the text
        return any(kw in txt for kw in keywords)
    

if __name__ == "__main__":
    # Example usage of the RecurringEntriesDetector
    ddf = pd.read_excel("/home/whirldata/Downloads/Recurring Entries - Main.xlsx", sheet_name="Data")
    ddf.rename(columns={"Transaction Text":"TRANSACTION_DESCRIPTION","Amount in Document Currency":"AMOUNT","Debit/Credit Indicator":"DEBIT_CREDIT_INDICATOR",
                        "GL Account ID":"ACCOUNT_CODE","Journal ID":"ACCOUNTDOCID","Accounting/Effective Date":"POSTED_DATE",
                        "Credit Amount":"CREDIT_AMOUNT","Debit Amount":"DEBIT_AMOUNT"}, inplace=True)

    recc_df = pd.read_csv("/home/whirldata/Downloads/recurring_entry/recc_df.csv")
    recc_df.rename(columns={"Frequency":"frequency","amount__deviation":"amount_deviation"}, inplace=True)
    recc_df['keywords'] = recc_df['keywords'].fillna('').astype(str)

    recc = RecurringEntriesDetector(transactions_df=ddf, recurring_df=recc_df, chart_accounts_df=pd.DataFrame())
    result = recc.detect()
    print(result['is_present'].value_counts())