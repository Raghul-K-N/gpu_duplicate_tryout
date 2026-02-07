from invoice_number_similarity.predict import make_prediction
from duplicate_invoices.config import config
from tqdm import tqdm
from pandarallel import pandarallel
import os
from duplicate_invoices.config import config
no_of_workers = os.getenv('NO_OF_CORES')
if no_of_workers is None:
    no_of_workers = 32
no_of_workers = int(no_of_workers)-1
pandarallel.initialize(progress_bar=True, nb_workers=no_of_workers)
tqdm.pandas()


def is_similar(source, dest, exact_matching=config.EXACT_MATCHING, model=config.INVOICE_NUMBER_SIMILARITY_MODEL):
    if exact_matching:
        return source==dest, int(source==dest)
    
    result = make_prediction(input_data=(source, dest), model=model)
    duplicate_risk_score = result['predictions'][1]
    similarity_score = result['similarity_score']

    return duplicate_risk_score, similarity_score


def get_similar_invoices(invoices, keys,flag, threshold=0.5, \
    exact_matching=config.EXACT_MATCHING, model=config.INVOICE_NUMBER_SIMILARITY_MODEL):
    # TODO: Clustering approach for grouping, maybe one invoice in two groups
    similars = []
    found = set()
    for i, (source, source_key,source_flag) in enumerate(zip(invoices, keys,flag)):
        if source_key in found:
            continue
        current_similars = set()
        current_similars_scores = []
        current_similars_risk_scores = []
        for j, (dest, dest_key,dest_flag) in enumerate(zip(invoices[i+1:], keys[i+1:],flag[i+1:])):
            # Commented because two invoice numbers (same)
            # Solved using key instead of invoice number
            if dest_key in found:
                continue
            if not (source_flag or dest_flag):
                continue
            current_risk_score, similarity_score = is_similar(source, dest, exact_matching=exact_matching, model=model)
            current_risk_score_rev, similarity_score_rev = is_similar(dest, source, exact_matching=exact_matching, model=model)
            current_risk_score = max(current_risk_score, current_risk_score_rev)
            similarity_score = max(similarity_score, similarity_score_rev)
            if current_risk_score > threshold:
                current_similars.add(source_key)
                current_similars.add(dest_key)
                current_similars_risk_scores.append(current_risk_score)
                current_similars_scores.append(similarity_score)
                found.add(source_key)
                found.add(dest_key)
        if len(current_similars):
            similars.append((current_similars, current_similars_risk_scores, current_similars_scores))
    return similars


def duplicate_invoicenumber_similar(df, column, grouping_columns, identifier, \
    exact_matching=config.EXACT_MATCHING, model=config.INVOICE_NUMBER_SIMILARITY_MODEL):
    t = df.groupby(list(grouping_columns))[[column, 'PrimaryKeySimple','is_current_data']].agg(lambda x: list(x)).reset_index()
    dupl = t[t['PrimaryKeySimple'].apply(lambda x: len(x)>1)]
    dupl['length'] = dupl['PrimaryKeySimple'].apply(len)
    dupl = dupl[dupl['length']<1000]
    if dupl.shape[0] > 0:
        dupl['DUPLICATES'] = dupl[[column, 'PrimaryKeySimple','is_current_data']].parallel_apply(lambda x: \
           get_similar_invoices(x[column], x['PrimaryKeySimple'], x['is_current_data'], exact_matching=exact_matching, model=model,threshold=config.THRESHOLD_VALUE), axis=1)
        # dupl['DUPLICATES'] = dupl[[column, 'PrimaryKeySimple']].progress_apply(lambda x: \
        #     get_similar_invoices(x[column], x['PrimaryKeySimple'], exact_matching=exact_matching, model=model), axis=1)
    


        actual_dupl = dupl[dupl['DUPLICATES'].apply(lambda x: len(x)>0)]

        dupl_dict = dict()
        dupl_risk_score_dict = dict()
        dupl_similarity_score_dict = dict()

        duplicate_id_count = 0
        for dl in actual_dupl['DUPLICATES'].to_list():
            for l in dl:
                # duplicate = ','.join(l[0])
                for item in list(l[0]):
                    dupl_dict[item] = duplicate_id_count #duplicate
                    dupl_risk_score_dict[item] = l[1]
                    dupl_similarity_score_dict[item] = l[2]
                duplicate_id_count += 1

        df[f'DUPLICATE_ID_{identifier}'] = df['PrimaryKeySimple'].map(dupl_dict)
        df[f'DUPLICATE_ID_{identifier}'].fillna(-1, inplace=True)
        df[f'DUPLICATE_ID_{identifier}'] = df[f'DUPLICATE_ID_{identifier}'].astype(int)
        df[f'DUPLICATE_RISK_SCORE_{identifier}'] = df['PrimaryKeySimple'].map(dupl_risk_score_dict)
        df[f'DUPLICATE_RISK_SCORE_{identifier}'].fillna(0, inplace=True)
        df[f'DUPLICATE_SIMILARITY_SCORE_{identifier}'] = df['PrimaryKeySimple'].map(dupl_similarity_score_dict)
        df[f'DUPLICATE_SIMILARITY_SCORE_{identifier}'].fillna(0, inplace=True)
        df[f'DUPLICATE_FLAG_{identifier}'] = df[f'DUPLICATE_ID_{identifier}'].apply(lambda x: x!=-1)

        return df
    else:

        return dupl