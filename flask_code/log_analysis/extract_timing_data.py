#!/usr/bin/env python3
"""
Script to extract timing data from log files and save to CSV.
Extracts timing information for each account document processing.
"""

import re
import pandas as pd
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_timing_data(log_file_path):
    """
    Extract timing data from log file.
    
    Args:
        log_file_path (str): Path to the log file
        
    Returns:
        list: List of dictionaries containing extracted data
    """
    # Regex pattern to match the timing log entries
    # Pattern: Time taken: XX.XX seconds for transaction XXXX and account document: ('acc_doc', 'client', 'company_code', np.int64(year))
    pattern = r"Time taken: ([\d.]+) seconds for transaction (\d+) and account document: \('([^']+)', '([^']+)', '([^']+)', np\.int64\((\d+)\)\)"
    
    extracted_data = []
    
    try:
        # Try different encodings to handle the file
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        file_content = None
        
        for encoding in encodings:
            try:
                with open(log_file_path, 'r', encoding=encoding) as file:
                    file_content = file.readlines()
                    logger.info(f"Successfully read file with encoding: {encoding}")
                    break
            except UnicodeDecodeError:
                continue
        
        if file_content is None:
            logger.error("Could not read file with any supported encoding")
            return []
            
        line_number = 0
        for line in file_content:
            line_number += 1
            match = re.search(pattern, line)
            if match:
                    time_taken = float(match.group(1))
                    transaction_id = int(match.group(2))
                    acc_doc = match.group(3)
                    client = match.group(4)
                    company_code = match.group(5)
                    year = int(match.group(6))
                    
                    extracted_data.append({
                        'acc_doc': acc_doc,
                        'client': client,
                        'company_code': company_code,
                        'year': year,
                        'time_taken': time_taken,
                        'transaction_id': transaction_id,
                        'log_line': line_number
                    })
                    
                    # Log progress every 100 records
                    if len(extracted_data) % 100 == 0:
                        logger.info(f"Extracted {len(extracted_data)} records so far...")
                        
    except FileNotFoundError:
        logger.error(f"Log file not found: {log_file_path}")
        return []
    except Exception as e:
        logger.error(f"Error reading log file: {e}")
        return []
    
    return extracted_data

def save_to_csv(data, output_path):
    """
    Save extracted data to CSV file.
    
    Args:
        data (list): List of dictionaries containing extracted data
        output_path (str): Path to save the CSV file
    """
    if not data:
        logger.warning("No data to save")
        return
    
    try:
        df = pd.DataFrame(data)
        
        # Reorder columns for better readability
        columns_order = ['acc_doc', 'client', 'company_code', 'year', 'time_taken', 'transaction_id', 'log_line']
        df = df[columns_order]
        
        # Sort by time_taken in descending order to see longest processing times first
        df = df.sort_values('time_taken', ascending=False)
        
        # Save to CSV
        df.to_csv(output_path, index=False)
        
        logger.info(f"Successfully saved {len(df)} records to {output_path}")
        
        # Print summary statistics
        logger.info(f"Summary statistics:")
        logger.info(f"  Total records: {len(df)}")
        logger.info(f"  Average time taken: {df['time_taken'].mean():.2f} seconds")
        logger.info(f"  Minimum time taken: {df['time_taken'].min():.2f} seconds")
        logger.info(f"  Maximum time taken: {df['time_taken'].max():.2f} seconds")
        logger.info(f"  Total processing time: {df['time_taken'].sum():.2f} seconds ({df['time_taken'].sum()/60:.2f} minutes)")
        
    except Exception as e:
        logger.error(f"Error saving to CSV: {e}")

def main():
    """Main function to execute the extraction process."""
    
    # Hardcoded log file path as requested
    log_file_path = r"c:\Users\ShriramSrinivasan\Desktop\dow_transformation\dow-transformation-mlvm\flask_code\invoice_verification\iv_logs\20251127_061345_1\General_log_20251127_061345_1.log"
    
    # Output CSV file path
    output_csv_path = r"c:\Users\ShriramSrinivasan\Desktop\dow_transformation\dow-transformation-mlvm\flask_code\timing_analysis_results.csv"
    
    logger.info(f"Starting extraction from: {log_file_path}")
    
    # Extract timing data
    extracted_data = extract_timing_data(log_file_path)
    
    if extracted_data:
        logger.info(f"Successfully extracted {len(extracted_data)} timing records")
        
        # Save to CSV
        save_to_csv(extracted_data, output_csv_path)
        
        # Display first few records as preview
        if len(extracted_data) > 0:
            logger.info("\nFirst 5 records preview:")
            df_preview = pd.DataFrame(extracted_data[:5])
            print(df_preview.to_string(index=False))
            
    else:
        logger.warning("No timing data found in the log file")

if __name__ == "__main__":
    main()