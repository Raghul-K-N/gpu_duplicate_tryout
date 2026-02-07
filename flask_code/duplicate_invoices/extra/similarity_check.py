# from code1.logger import logger
from datetime import datetime, timezone
from code1.logger import capture_log_message

def similarity_check(df, check_column, grouping_column, which_one):
    col_name = 'DUPLICATES_' + str(which_one)
    df[col_name] = ""
    subset_df = df[df[check_column] == 1]
    all_duplicate = []
    lengths = []
    capture_log_message(log_message="Starting fuzzy string matching...")
    start_fuzzy_matching = datetime.now(timezone.utc)
    for distinct_val in tqdm(set(subset_df[grouping_column])):
        subset = subset_df[subset_df[grouping_column] == distinct_val]
        invoices_set = set(list(subset['INVOICE_NUMBER']))
        lengths.append(len(invoices_set))

        for inv in invoices_set:
            matches = process.extract(inv, invoices_set, scorer=fuzz.ratio, score_cutoff=60)
            matches_invoices = [m[0] for m in matches]
            all_duplicate += matches_invoices
            if len(matches):
                all_duplicate.append(inv)

    finish_fuzzy_matching = datetime.now(timezone.utc)
    capture_log_message(log_message='Time taken for string matching: ' + str(finish_fuzzy_matching - start_fuzzy_matching))
    capture_log_message(log_message="Average number of strings to match: " + str(sum(lengths) / len(lengths)))

    capture_log_message(log_message="Updating dataframe...")
    start_dataframe_update = datetime.now(timezone.utc)
    all_dict = df.groupby('INVOICE_NUMBER')['INVOICE_ID_COPY'].agg(lambda x: list(x)).to_dict()
    string_dict = dict()

    for p in tqdm(set(all_duplicate)):
        string_dict[p] = ",".join(all_dict[p])

    df[col_name] = df['INVOICE_NUMBER'].map(string_dict)
    df[col_name].fillna("")
    finish_dataframe_update = datetime.now(timezone.utc)
    capture_log_message(log_message="Time taken for updating dataframe: " + str(finish_dataframe_update - start_dataframe_update))
    df['DUPLICATE_INV_' + str(which_one)] = np.where(~(df[col_name] == ""), 1, 0)