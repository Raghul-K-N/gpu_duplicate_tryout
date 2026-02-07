import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
import xgboost as xgb
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Load the dataset
df = pd.read_excel('meta_classifier_dataset.xlsx')

# Feature Engineering
def create_features(df):
    # Create a copy of the dataframe
    df_new = df.copy()
    
    # 1. Calculate absolute difference in amounts
    df_new['amount_diff'] = abs(df_new['invoice_amount_1'] - df_new['invoice_amount_2'])
    
    # 2. Calculate date difference in days
    df_new['date_diff'] = (df_new['invoice_date_1'] - df_new['invoice_date_2']).dt.days.abs()
    
    # 3. Calculate string similarity for vendor names
    from difflib import SequenceMatcher
    def string_similarity(a, b):
        return SequenceMatcher(None, str(a), str(b)).ratio()
    
    df_new['vendor_similarity'] = df_new.apply(
        lambda x: string_similarity(x['vendor_name_1'], x['vendor_name_2']), axis=1
    )
    
    # 4. Calculate string similarity for invoice numbers
    df_new['invoice_similarity'] = df_new.apply(
        lambda x: string_similarity(x['invoice_number_1'], x['invoice_number_2']), axis=1
    )
    
    # 5. Create interaction features
    df_new['amount_date_interaction'] = df_new['amount_diff'] * df_new['date_diff']
    df_new['vendor_amount_interaction'] = df_new['vendor_similarity'] * df_new['amount_diff']
    
    # 6. Create risk score interactions
    df_new['risk_amount_interaction'] = df_new['risk_score'] * df_new['amount_diff']
    df_new['risk_date_interaction'] = df_new['risk_score'] * df_new['date_diff']
    
    # Select features for training
    feature_columns = [
        'is_same_vendor', 'is_same_invoice_amount', 'is_same_invoice_date',
        'risk_score', 'amount_diff', 'date_diff', 'vendor_similarity',
        'invoice_similarity', 'amount_date_interaction', 'vendor_amount_interaction',
        'risk_amount_interaction', 'risk_date_interaction'
    ]
    
    return df_new[feature_columns], df_new['user_feedback']

# Create features
X, y = create_features(df)

# Split the data
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print('Shape of training features:', X_train.shape)
print('Shape of test features:', X_test.shape)
print('Shape of training labels:', y_train.shape)
print('Shape of test labels:', y_test.shape)


# Scale the features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Calculate class weights using only training data
n_samples = len(y_train)
n_pos = sum(y_train == 1)
n_neg = sum(y_train == 0)
scale_pos_weight = n_neg / n_pos  # This will give more weight to minority class
print('Scale importance:', scale_pos_weight)



# Calculate metrics
def print_metrics(y_true, y_pred, dataset_name):
    try:
        print(f"\nMetrics for {dataset_name}:")
        print(f"Accuracy: {accuracy_score(y_true, y_pred):.4f}")
        print(f"Precision: {precision_score(y_true, y_pred):.4f}")
        print(f"Recall: {recall_score(y_true, y_pred):.4f}")
        print(f"F1 Score: {f1_score(y_true, y_pred):.4f}")
        
        # Print confusion matrix
        cm = confusion_matrix(y_true, y_pred)
        plt.figure(figsize=(8, 6))
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
        plt.title(f'Confusion Matrix - {dataset_name}')
        plt.ylabel('True Label')
        plt.xlabel('Predicted Label')
        plt.savefig(f'confusion_matrix_{dataset_name.lower().replace(" ", "_")}.png')
        plt.close()
    except Exception as e:
        print(f"Error calculating metrics for {dataset_name}: {str(e)}") 

# Initialize and train XGBoost model
model = xgb.XGBClassifier(
    n_estimators=100,
    learning_rate=0.1,
    max_depth=5,
    min_child_weight=1,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42,
    scale_pos_weight=scale_pos_weight,  # Now gives more importance to minority class (0)
    eval_metric=['logloss', 'auc']
)

# Train the model using only training data
try:
    model.fit(
        X_train_scaled, y_train,
        eval_set=[(X_train_scaled, y_train)],  # Only use training data for evaluation during training
        early_stopping=10,
        verbose=True
    )
except Exception as e:
    print(f"Error during model training: {str(e)}")
    print("Trying alternative fit method...")
    # Fallback to simpler fit method if the above fails
    model.fit(
        X_train_scaled, y_train,
        verbose=True
    )

# Make predictions on training data for model evaluation
try:
    y_train_pred = model.predict(X_train_scaled)
except Exception as e:
    print(f"Error during training prediction: {str(e)}")
    raise

# Calculate metrics for training data
print_metrics(y_train, y_train_pred, "Training Set")

# Feature importance using only training data
try:
    importance = model.feature_importances_
    feature_importance = pd.DataFrame({
        'feature': X.columns,
        'importance': importance
    })
    feature_importance = feature_importance.sort_values('importance', ascending=False)

    # Plot feature importance
    plt.figure(figsize=(10, 6))
    sns.barplot(x='importance', y='feature', data=feature_importance)
    plt.title('Feature Importance')
    plt.tight_layout()
    plt.savefig('feature_importance.png')
    plt.close()

    # Save feature importance to CSV
    feature_importance.to_csv('feature_importance.csv', index=False)
except Exception as e:
    print(f"Error calculating feature importance: {str(e)}")

# Save the model
try:
    model.save_model('meta_classifier_model.json')
    print("\nModel and results have been saved successfully!")
except Exception as e:
    print(f"Error saving model: {str(e)}")

# Final evaluation on test data (only after model is fully trained and saved)
print("\nFinal Model Evaluation on Test Data:")
y_test_pred = model.predict(X_test_scaled)
print_metrics(y_test, y_test_pred, "Test Set")

# Make predictions
try:
    y_train_pred = model.predict(X_train_scaled)
    y_test_pred = model.predict(X_test_scaled)
except Exception as e:
    print(f"Error during prediction: {str(e)}")
    raise
