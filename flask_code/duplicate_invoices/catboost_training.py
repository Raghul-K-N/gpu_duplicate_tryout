"""Cat Boost Model Training"""

import numpy as np
import logging
from rapidfuzz.distance import Levenshtein
import pandas as pd
import networkx as nx
from itertools import chain, combinations
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, f1_score
from sklearn.model_selection import GridSearchCV
from datetime import datetime
import logging
import joblib
from catboost import CatBoostClassifier
from sklearn.pipeline import Pipeline
import json
from invoice_number_similarity.processing.features import extract_features


# The below code was run manaully to train catboost model using labeled data and store the trained model in a file


two_k_df = pd.read_excel('/home/whirldata/Music/catboost_test_data_manual_2.xlsx')
fea_json = '/invoice_number_similarity/datasets/columns_to_take.json'

def generate_invoice_pairs(df, amount_col, date_col, duplicate_col, invoice_number):
    
    
    amount_col_abs = f'{amount_col}_abs'
    df[amount_col_abs] = np.abs(df[amount_col])  # Compute absolute values for amounts
    df[invoice_number] = df[invoice_number].astype(str)
    # Step 2: Generate duplicate pairs
    duplicate_pairs = []
    for _, grp in df[df[duplicate_col] == 1].groupby([amount_col_abs, date_col]):
        if len(grp) > 1:  # Only process groups with more than 1 invoice
            for pair in combinations(grp.index, 2):
                duplicate_pairs.append((*pair, 1))  # Label as duplicate (1)

    # Step 3: Generate non-duplicate pairs
    non_dup_pairs = []
    for _, grp in df[df[duplicate_col] == 0].groupby([amount_col_abs]):
        if len(grp) > 1:  # Only process groups with more than 1 invoice
            for pair in combinations(grp.index, 2):
                non_dup_pairs.append((*pair, 0))  # Label as non-duplicate (0)

    # Step 4: Combine duplicate and non-duplicate pairs
    final_pairs = duplicate_pairs + non_dup_pairs
    print(f"Total pairs generated: {len(final_pairs)}")

    # Print percentages of duplicates and non-duplicates
    duplicate_count = len(duplicate_pairs)
    non_duplicate_count = len(non_dup_pairs)
    total_pairs = duplicate_count + non_duplicate_count

    duplicate_percent = (duplicate_count / total_pairs) * 100 if total_pairs > 0 else 0
    non_duplicate_percent = (non_duplicate_count / total_pairs) * 100 if total_pairs > 0 else 0

    print(f"Duplicate pairs: {duplicate_count} ({duplicate_percent:.2f}%)")
    print(f"Non-duplicate pairs: {non_duplicate_count} ({non_duplicate_percent:.2f}%)")
    
    # Step 5: Map indices to invoice numbers
    index_to_invoice_number_dict = dict(df[invoice_number])  # Create mapping dictionary
    pairs_df = pd.DataFrame(final_pairs, columns=['id1', 'id2', 'label'])

    # Map invoice numbers
    pairs_df['inv1'] = pairs_df['id1'].map(index_to_invoice_number_dict)
    pairs_df['inv2'] = pairs_df['id2'].map(index_to_invoice_number_dict)

    return pairs_df

def add_features_to_pairs(pairs_df):
    with open(fea_json, 'r') as f:
        features_dict = json.load(f)
    features = features_dict['categorical'] + features_dict['numerical']
    print(len(features))
    for col in features:
        if col not in pairs_df.columns:
            pairs_df[col] = -1

    # for col in features:
    #     pairs_df[f'{col}_1'] = pairs_df['id1'].map(df[col])
    #     pairs_df[f'{col}_2'] = pairs_df['id2'].map(df[col])

    return pairs_df

def update_df_with_features(df, src, dest, index):
    # Extract features
    features = extract_features(src, dest)

    # Update DataFrame with extracted features
    for feature, value in features.items():
        if feature in df.columns:
            df.at[index, feature] = value
        else:
            # Add new column if feature doesn't exist
            df[feature] = -1
            df.at[index, feature] = value

def process_features_df(features_df):
    for i, row in features_df.iterrows():
        # Call the feature update function
        update_df_with_features(
            df=features_df,
            src=str(row['inv1']),
            dest=str(row['inv2']),
            index=i
        )
    return features_df 

model = CatBoostClassifier(iterations=300,
                            depth=8,
                            learning_rate=0.1,
                            random_state=42,
                            loss_function='Logloss',
                            verbose=True)

def run_training(df) -> None:
    """Train the model."""
    try:
        # read training data
        data = df.copy()

        with open(fea_json, 'r') as f:
            features_dict = json.load(f)
        features = features_dict['categorical'] + features_dict['numerical']

        # divide train and test
        target_col = 'label'
        X_train, X_test, y_train, y_test = train_test_split(
            data[features], data[target_col], test_size=0.3, random_state=42, stratify=data['label']
        )  
        print("\nWith Stratify:")
        print("y_train:", y_train.value_counts())
        print("y_test:", y_test.value_counts())
        model.fit(X_train, y_train)
        print(model.learning_rate_)

        print(model.get_all_params())

        # Evaluate model
        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        report = classification_report(y_test, y_pred)
        f1 = f1_score(y_test, y_pred)  # y_test is the true label (y_true)
        print(f"F1 Score: {f1:.2f}")

        # Log results
        print(f"Model accuracy: {accuracy:.4f}")
        print("Classification Report:\n" + report)
        # print(f"Feature Imp: {model.feature_importances_}")
        
        # Save model
        model_filename = f"model_cat_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl"
        joblib.dump(model, model_filename)
        print(f"Model saved as: {model_filename}")
        print(f"Model saved as: {model_filename}")
        # return model, X_train
    except Exception as e:
        print(f"Training failed: {str(e)}")
        raise

def main():
    pairs_df = generate_invoice_pairs(df=two_k_df, date_col='DocDate', amount_col='Amountindoccurr',duplicate_col='DUPLICATE', invoice_number='Reference')

    features_df = add_features_to_pairs(pairs_df)

    featured_df = features_df.copy()

    final_df = process_features_df(featured_df)

    run_training(final_df)