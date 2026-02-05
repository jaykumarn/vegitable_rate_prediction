"""
Crop Rate Analysis for March
Analyzes historical data to determine which crops give the best rates in March.
"""

import pandas as pd
import re


def extract_rate(rate_string):
    """Extract numeric rate value from string like 'Rs. 1700/-'"""
    if pd.isna(rate_string):
        return None
    match = re.search(r'(\d+)', str(rate_string))
    return int(match.group(1)) if match else None


def load_and_prepare_data(filepath):
    """Load Excel data and prepare for analysis."""
    df = pd.read_excel(filepath)
    
    # Extract numeric rates
    df['max_rate'] = df['product_max_rate'].apply(extract_rate)
    df['min_rate'] = df['product_min_rate'].apply(extract_rate)
    
    # Calculate average rate
    df['avg_rate'] = (df['max_rate'] + df['min_rate']) / 2
    
    # Extract month
    df['month'] = df['rate_date'].dt.month
    df['year'] = df['rate_date'].dt.year
    
    return df


def analyze_march_rates(df):
    """Analyze crop rates for March."""
    march_data = df[df['month'] == 3].copy()
    
    # Aggregate by crop
    crop_stats = march_data.groupby('product_name').agg({
        'max_rate': ['mean', 'max', 'min', 'std'],
        'min_rate': ['mean', 'max', 'min'],
        'avg_rate': 'mean',
        'rate_date': 'count'
    }).round(2)
    
    # Flatten column names
    crop_stats.columns = [
        'avg_max_rate', 'highest_max_rate', 'lowest_max_rate', 'max_rate_std',
        'avg_min_rate', 'highest_min_rate', 'lowest_min_rate',
        'avg_rate', 'record_count'
    ]
    
    # Sort by average rate (descending)
    crop_stats = crop_stats.sort_values('avg_rate', ascending=False)
    
    return crop_stats, march_data


def main():
    filepath = '/tmp/inputs/product_all.xlsx'
    
    print("=" * 70)
    print("CROP RATE ANALYSIS FOR MARCH")
    print("=" * 70)
    
    # Load data
    df = load_and_prepare_data(filepath)
    
    # Analyze March data
    crop_stats, march_data = analyze_march_rates(df)
    
    print(f"\nData period: {df['rate_date'].min().date()} to {df['rate_date'].max().date()}")
    print(f"Total March records analyzed: {len(march_data)}")
    print(f"Number of crops: {len(crop_stats)}")
    
    # Top crops by average rate
    print("\n" + "=" * 70)
    print("TOP 15 CROPS WITH HIGHEST AVERAGE RATE IN MARCH")
    print("=" * 70)
    
    top_crops = crop_stats.head(15)[['avg_rate', 'avg_max_rate', 'avg_min_rate', 'record_count']]
    print(top_crops.to_string())
    
    # Best crop
    best_crop = crop_stats.index[0]
    best_rate = crop_stats.iloc[0]['avg_rate']
    
    print("\n" + "=" * 70)
    print("RECOMMENDATION")
    print("=" * 70)
    print(f"\nBased on historical data, '{best_crop}' gives the HIGHEST rate in March")
    print(f"Average Rate: Rs. {best_rate:.2f}")
    print(f"Average Max Rate: Rs. {crop_stats.iloc[0]['avg_max_rate']:.2f}")
    print(f"Average Min Rate: Rs. {crop_stats.iloc[0]['avg_min_rate']:.2f}")
    
    # Top 5 summary
    print("\n" + "-" * 70)
    print("TOP 5 CROPS FOR MARCH (by average rate):")
    print("-" * 70)
    for i, (crop, row) in enumerate(crop_stats.head(5).iterrows(), 1):
        print(f"{i}. {crop}: Rs. {row['avg_rate']:.2f} (Max: Rs. {row['avg_max_rate']:.2f}, Min: Rs. {row['avg_min_rate']:.2f})")
    
    # Save detailed results
    output_path = '/tmp/inputs/march_crop_rates.csv'
    crop_stats.to_csv(output_path)
    print(f"\nDetailed results saved to: {output_path}")


if __name__ == '__main__':
    main()
