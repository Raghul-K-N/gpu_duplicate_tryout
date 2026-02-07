#!/usr/bin/env python3
"""
Script to analyze the extracted timing data and provide insights.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

def analyze_timing_data(csv_path):
    """
    Analyze the timing data and generate insights.
    
    Args:
        csv_path (str): Path to the CSV file with timing data
    """
    try:
        # Read the CSV file
        df = pd.read_csv(csv_path)
        
        print("=" * 60)
        print("TIMING ANALYSIS REPORT")
        print("=" * 60)
        
        # Basic statistics
        print(f"\n📊 BASIC STATISTICS:")
        print(f"Total records processed: {len(df):,}")
        print(f"Average processing time: {df['time_taken'].mean():.2f} seconds")
        print(f"Median processing time: {df['time_taken'].median():.2f} seconds")
        print(f"Standard deviation: {df['time_taken'].std():.2f} seconds")
        print(f"Minimum time: {df['time_taken'].min():.2f} seconds")
        print(f"Maximum time: {df['time_taken'].max():.2f} seconds")
        print(f"Total processing time: {df['time_taken'].sum()/60:.2f} minutes ({df['time_taken'].sum()/3600:.2f} hours)")
        
        # Performance categories
        print(f"\n⚡ PERFORMANCE BREAKDOWN:")
        fast = df[df['time_taken'] < 10]
        medium = df[(df['time_taken'] >= 10) & (df['time_taken'] < 60)]
        slow = df[(df['time_taken'] >= 60) & (df['time_taken'] < 180)]
        very_slow = df[df['time_taken'] >= 180]
        
        print(f"Fast (< 10s): {len(fast):,} records ({len(fast)/len(df)*100:.1f}%)")
        print(f"Medium (10-60s): {len(medium):,} records ({len(medium)/len(df)*100:.1f}%)")
        print(f"Slow (60-180s): {len(slow):,} records ({len(slow)/len(df)*100:.1f}%)")
        print(f"Very Slow (>180s): {len(very_slow):,} records ({len(very_slow)/len(df)*100:.1f}%)")
        
        # Top 10 slowest processing times
        print(f"\n🐌 TOP 10 SLOWEST PROCESSING TIMES:")
        top_10_slowest = df.nlargest(10, 'time_taken')
        for idx, row in top_10_slowest.iterrows():
            print(f"{row['acc_doc']} (Client: {row['client']}, Company: {row['company_code']}) - {row['time_taken']:.2f}s")
        
        # Analysis by company code
        print(f"\n🏢 ANALYSIS BY COMPANY CODE:")
        company_stats = df.groupby('company_code').agg({
            'time_taken': ['count', 'mean', 'sum']
        }).round(2)
        company_stats.columns = ['Count', 'Avg_Time', 'Total_Time']
        company_stats = company_stats.sort_values('Avg_Time', ascending=False)
        print(company_stats.head(10))
        
        # Analysis by client
        print(f"\n👥 ANALYSIS BY CLIENT:")
        client_stats = df.groupby('client').agg({
            'time_taken': ['count', 'mean', 'sum']
        }).round(2)
        client_stats.columns = ['Count', 'Avg_Time', 'Total_Time']
        print(client_stats)
        
        # Percentile analysis
        print(f"\n📈 PERCENTILE ANALYSIS:")
        percentiles = [50, 75, 90, 95, 99]
        for p in percentiles:
            value = df['time_taken'].quantile(p/100)
            print(f"{p}th percentile: {value:.2f} seconds")
        
        # Documents that might need optimization
        print(f"\n🎯 OPTIMIZATION CANDIDATES (>2 std deviations):")
        mean_time = df['time_taken'].mean()
        std_time = df['time_taken'].std()
        threshold = mean_time + 2 * std_time
        optimization_candidates = df[df['time_taken'] > threshold]
        
        if len(optimization_candidates) > 0:
            print(f"Found {len(optimization_candidates)} documents taking >{threshold:.2f}s:")
            for idx, row in optimization_candidates.iterrows():
                print(f"  {row['acc_doc']} - {row['time_taken']:.2f}s")
        else:
            print("No significant outliers found.")
            
        print(f"\n📁 Results saved to: {csv_path}")
        print("=" * 60)
        
    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_path}")
    except Exception as e:
        print(f"Error analyzing data: {e}")

def main():
    """Main function to execute the analysis."""
    csv_path = r"c:\Users\ShriramSrinivasan\Desktop\dow_transformation\dow-transformation-mlvm\flask_code\timing_analysis_results.csv"
    analyze_timing_data(csv_path)

if __name__ == "__main__":
    main()