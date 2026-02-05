import pandas as pd
import re


def clean_rate(rate_str):
    """Extract numeric value from rate string like 'Rs. 1700/-'"""
    if pd.isna(rate_str) or rate_str == '&nbsp;':
        return None
    match = re.search(r'(\d+)', str(rate_str))
    if match:
        return float(match.group(1))
    return None


def analyze_march_crop_rates(file_path):
    """Analyze which crop gives the best rate in March based on historical data."""
    
    df = pd.read_excel(file_path)
    
    # Clean rate columns
    df['max_rate'] = df['product_max_rate'].apply(clean_rate)
    df['min_rate'] = df['product_min_rate'].apply(clean_rate)
    
    # Calculate average rate
    df['avg_rate'] = (df['max_rate'] + df['min_rate']) / 2
    
    # Extract month
    df['month'] = df['rate_date'].dt.month
    
    # Filter for March (month = 3)
    march_data = df[df['month'] == 3].copy()
    
    # Group by product and calculate average rates
    march_summary = march_data.groupby('product_name').agg({
        'max_rate': 'mean',
        'min_rate': 'mean',
        'avg_rate': 'mean'
    }).round(2)
    
    # Sort by average rate descending
    march_summary = march_summary.sort_values('avg_rate', ascending=False)
    
    # Rename columns for clarity
    march_summary.columns = ['Avg Max Rate (Rs)', 'Avg Min Rate (Rs)', 'Average Rate (Rs)']
    
    return march_summary


if __name__ == '__main__':
    result = analyze_march_crop_rates('product_all.xlsx')
    
    print("=" * 70)
    print("CROP RATE ANALYSIS FOR MARCH (Based on Historical Data)")
    print("=" * 70)
    print()
    
    print("TOP 10 CROPS WITH HIGHEST RATES IN MARCH:")
    print("-" * 70)
    print(result.head(10).to_string())
    print()
    
    print("=" * 70)
    best_crop = result.index[0]
    best_rate = result.iloc[0]['Average Rate (Rs)']
    print(f"RECOMMENDATION: '{best_crop}' gives the highest rate in March")
    print(f"Expected Average Rate: Rs. {best_rate}")
    print("=" * 70)
    
    print("\n\nFULL RANKING OF ALL CROPS FOR MARCH:")
    print("-" * 70)
    print(result.to_string())
