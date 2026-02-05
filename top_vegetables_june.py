"""
Find top 5 vegetables with highest rate and volume in June.
Rate and Volume are given equal priority (50% each) using min-max normalization.
"""

import pandas as pd
import numpy as np


def extract_rate(rate_str):
    """Extract numeric rate value from string like 'Rs. 1700/-'"""
    if pd.isna(rate_str):
        return np.nan
    rate_str = str(rate_str)
    cleaned = rate_str.replace('Rs.', '').replace('/-', '').replace(',', '').strip()
    try:
        return float(cleaned)
    except ValueError:
        return np.nan


def normalize(series):
    """Min-max normalization to scale values between 0 and 1"""
    min_val = series.min()
    max_val = series.max()
    if max_val == min_val:
        return pd.Series([0.5] * len(series), index=series.index)
    return (series - min_val) / (max_val - min_val)


def main():
    df = pd.read_excel('product_all.xlsx')
    
    # Filter for June (any year) and vegetables (code 2001-2999)
    june_veggies = df[
        (df['rate_date'].dt.month == 6) & 
        (df['code_number'] >= 2001) & 
        (df['code_number'] <= 2999)
    ].copy()
    
    # Parse rate and volume
    june_veggies['max_rate'] = june_veggies['product_max_rate'].apply(extract_rate)
    june_veggies['min_rate'] = june_veggies['product_min_rate'].apply(extract_rate)
    june_veggies['avg_rate'] = (june_veggies['max_rate'] + june_veggies['min_rate']) / 2
    june_veggies['volume'] = pd.to_numeric(june_veggies['product_quantity'], errors='coerce')
    
    # Aggregate by vegetable
    agg_df = june_veggies.groupby('product_name').agg({
        'volume': 'sum',
        'avg_rate': 'mean'
    }).reset_index()
    
    # Normalize and compute combined score (equal weights)
    agg_df['rate_norm'] = normalize(agg_df['avg_rate'])
    agg_df['volume_norm'] = normalize(agg_df['volume'])
    agg_df['combined_score'] = 0.5 * agg_df['rate_norm'] + 0.5 * agg_df['volume_norm']
    
    # Get top 5
    top_5 = agg_df.nlargest(5, 'combined_score')
    
    # Display results
    print("=" * 70)
    print("TOP 5 VEGETABLES IN JUNE (Rate & Volume with Equal Priority)")
    print("=" * 70)
    print()
    
    for rank, (_, row) in enumerate(top_5.iterrows(), 1):
        print(f"Rank {rank}: {row['product_name']}")
        print(f"   Total Volume: {row['volume']:,.0f} units")
        print(f"   Average Rate: Rs. {row['avg_rate']:,.2f}")
        print(f"   Combined Score: {row['combined_score']:.4f}")
        print()
    
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    summary = top_5[['product_name', 'volume', 'avg_rate', 'combined_score']].copy()
    summary.columns = ['Vegetable', 'Total Volume', 'Avg Rate (Rs.)', 'Score']
    summary = summary.reset_index(drop=True)
    summary.index = range(1, 6)
    print(summary.to_string())


if __name__ == '__main__':
    main()
