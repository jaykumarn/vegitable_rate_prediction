import pandas as pd
import numpy as np

def extract_rate(rate_str):
    """Extract numeric value from rate string like 'Rs. 1700/-'"""
    if pd.isna(rate_str):
        return np.nan
    rate_str = str(rate_str)
    # Remove 'Rs.', '/-', spaces, and commas
    cleaned = rate_str.replace('Rs.', '').replace('/-', '').replace(',', '').strip()
    try:
        return float(cleaned)
    except ValueError:
        return np.nan

def main():
    # Load data
    df = pd.read_excel('product_all.xlsx')
    
    # Filter for June (any year in the dataset)
    df['rate_date'] = pd.to_datetime(df['rate_date'])
    june_df = df[df['rate_date'].dt.month == 6].copy()
    
    # Filter for vegetables (code_number starting with '2')
    june_veg = june_df[june_df['code_number'].astype(str).str.startswith('2')].copy()
    
    print(f"Total records in June: {len(june_df)}")
    print(f"Vegetable records in June: {len(june_veg)}")
    print()
    
    # Extract numeric rates
    june_veg['max_rate'] = june_veg['product_max_rate'].apply(extract_rate)
    june_veg['min_rate'] = june_veg['product_min_rate'].apply(extract_rate)
    
    # Calculate average rate (average of max and min)
    june_veg['avg_rate'] = (june_veg['max_rate'] + june_veg['min_rate']) / 2
    
    # Volume is in product_quantity
    june_veg['volume'] = pd.to_numeric(june_veg['product_quantity'], errors='coerce')
    
    # Aggregate by product_name: sum of volume, mean of average rate
    agg_df = june_veg.groupby('product_name').agg({
        'volume': 'sum',
        'avg_rate': 'mean'
    }).reset_index()
    
    # Normalize rate and volume to 0-1 scale for equal priority ranking
    agg_df['rate_normalized'] = (agg_df['avg_rate'] - agg_df['avg_rate'].min()) / (agg_df['avg_rate'].max() - agg_df['avg_rate'].min())
    agg_df['volume_normalized'] = (agg_df['volume'] - agg_df['volume'].min()) / (agg_df['volume'].max() - agg_df['volume'].min())
    
    # Combined score with equal weights (50% rate, 50% volume)
    agg_df['combined_score'] = 0.5 * agg_df['rate_normalized'] + 0.5 * agg_df['volume_normalized']
    
    # Sort by combined score descending
    top_5 = agg_df.nlargest(5, 'combined_score')
    
    print("=" * 70)
    print("TOP 5 VEGETABLES IN JUNE (BY RATE AND VOLUME - EQUAL PRIORITY)")
    print("=" * 70)
    print()
    
    for i, (_, row) in enumerate(top_5.iterrows(), 1):
        print(f"{i}. {row['product_name']}")
        print(f"   Total Volume: {row['volume']:,.0f}")
        print(f"   Average Rate: Rs. {row['avg_rate']:,.2f}")
        print(f"   Combined Score: {row['combined_score']:.4f}")
        print(f"   (Rate Score: {row['rate_normalized']:.4f}, Volume Score: {row['volume_normalized']:.4f})")
        print()
    
    # Also show as a formatted table
    print("=" * 70)
    print("SUMMARY TABLE")
    print("=" * 70)
    result_table = top_5[['product_name', 'volume', 'avg_rate', 'combined_score']].copy()
    result_table.columns = ['Vegetable', 'Total Volume', 'Avg Rate (Rs.)', 'Score']
    result_table['Rank'] = range(1, 6)
    result_table = result_table[['Rank', 'Vegetable', 'Total Volume', 'Avg Rate (Rs.)', 'Score']]
    result_table['Total Volume'] = result_table['Total Volume'].apply(lambda x: f"{x:,.0f}")
    result_table['Avg Rate (Rs.)'] = result_table['Avg Rate (Rs.)'].apply(lambda x: f"{x:,.2f}")
    result_table['Score'] = result_table['Score'].apply(lambda x: f"{x:.4f}")
    print(result_table.to_string(index=False))

if __name__ == '__main__':
    main()
