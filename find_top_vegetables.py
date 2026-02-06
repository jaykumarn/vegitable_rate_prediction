import pandas as pd
import numpy as np

# Average yield per acre in quintals for common vegetables (Indian agricultural data)
YIELD_PER_ACRE = {
    'भेंडी': 40,        # Okra/Bhindi
    'गवार': 25,         # Cluster Beans
    'टोमॅटो': 100,      # Tomato
    'मटार': 30,         # Peas
    'घेवडा': 35,        # French Beans
    'दोडका': 50,        # Ridge Gourd
    'हि.मिरची': 60,     # Green Chilli
    'दुधीभोपळा': 80,    # Bottle Gourd
    'भु. शेंग': 8,      # Groundnut
    'काकडी': 60,        # Cucumber
    'कारली': 40,        # Bitter Gourd
    'डांगर': 50,        # Taro/Colocasia
    'गाजर': 80,         # Carrot
    'पापडी': 25,        # Hyacinth Bean
    'पडवळ': 50,         # Snake Gourd
    'फ्लॉवर': 60,       # Cauliflower
    'कोबी': 80,         # Cabbage
    'वांगी': 80,        # Brinjal/Eggplant
    'ढोबळी': 50,        # Capsicum
    'सुरण': 100,        # Elephant Foot Yam
    'तोंडली': 40,       # Ivy Gourd
    'बीट': 80,          # Beetroot
    'कोहळा': 100,       # Ash Gourd
    'पावटा': 30,        # Field Beans
    'वाल': 25,          # Hyacinth Bean (dry)
    'वालवर': 25,        # Hyacinth Bean (variety)
    'शेवगा': 40,        # Drumstick
    'कैरी': 50,         # Raw Mango
    'ढेमसा': 30,        # Spine Gourd
    'नवलकोल': 60,       # Kohlrabi
    'मुळा': 80,         # Radish
    'लसुन': 30,         # Garlic
    'कांदा पात': 50,    # Spring Onion
    'मेथी': 25,         # Fenugreek
    'पालक': 40,         # Spinach
    'कोथिंबीर': 30,     # Coriander
    'शेपु': 25,         # Dill
    'चवळी': 30,         # Cowpea
    'आंबा': 40,         # Mango
    'भोपळा': 80,        # Pumpkin
    'कांदा': 80,        # Onion
    'बटाटा': 80,        # Potato
    'आले': 60,          # Ginger
}

DEFAULT_YIELD = 50  # Default yield for unknown vegetables


def extract_rate(rate_str):
    """Extract numeric value from rate string like 'Rs. 1700/-'"""
    if pd.isna(rate_str):
        return np.nan
    rate_str = str(rate_str)
    cleaned = rate_str.replace('Rs.', '').replace('/-', '').replace(',', '').strip()
    try:
        return float(cleaned)
    except ValueError:
        return np.nan


def load_and_prepare_data(filepath):
    """Load excel data and prepare vegetable records"""
    df = pd.read_excel(filepath)
    df['rate_date'] = pd.to_datetime(df['rate_date'])
    
    # Filter vegetables (code_number starting with '2')
    veg_df = df[df['code_number'].astype(str).str.startswith('2')].copy()
    
    # Extract numeric rates
    veg_df['max_rate'] = veg_df['product_max_rate'].apply(extract_rate)
    veg_df['min_rate'] = veg_df['product_min_rate'].apply(extract_rate)
    veg_df['avg_rate'] = (veg_df['max_rate'] + veg_df['min_rate']) / 2
    
    # Add month column
    veg_df['month'] = veg_df['rate_date'].dt.month
    
    return veg_df


def calculate_profitability(veg_df):
    """Calculate profitability score for each vegetable per month"""
    # Aggregate by month and product: mean of average rate
    monthly_agg = veg_df.groupby(['month', 'product_name']).agg({
        'avg_rate': 'mean',
        'max_rate': 'mean',
        'min_rate': 'mean'
    }).reset_index()
    
    # Add yield per acre
    monthly_agg['yield_per_acre'] = monthly_agg['product_name'].map(YIELD_PER_ACRE).fillna(DEFAULT_YIELD)
    
    # Calculate estimated revenue per acre (rate is per quintal)
    monthly_agg['revenue_per_acre'] = monthly_agg['avg_rate'] * monthly_agg['yield_per_acre']
    
    return monthly_agg


def get_top_vegetables_by_month(profitability_df, top_n=5):
    """Get top N most profitable vegetables for each month"""
    results = {}
    month_names = {
        1: 'January', 2: 'February', 3: 'March', 4: 'April',
        5: 'May', 6: 'June', 7: 'July', 8: 'August',
        9: 'September', 10: 'October', 11: 'November', 12: 'December'
    }
    
    for month in sorted(profitability_df['month'].unique()):
        month_data = profitability_df[profitability_df['month'] == month]
        top_veg = month_data.nlargest(top_n, 'revenue_per_acre')[
            ['product_name', 'avg_rate', 'yield_per_acre', 'revenue_per_acre']
        ].reset_index(drop=True)
        top_veg.index = range(1, len(top_veg) + 1)
        results[month_names[month]] = top_veg
    
    return results


def print_results(results):
    """Print formatted results"""
    print("=" * 80)
    print("MOST PROFITABLE VEGETABLES BY MONTH (Based on Price × Yield per Acre)")
    print("=" * 80)
    
    for month_name, df in results.items():
        print(f"\n{month_name.upper()}")
        print("-" * 80)
        print(f"{'Rank':<6}{'Vegetable':<15}{'Avg Rate (Rs/Q)':<18}{'Yield (Q/Acre)':<16}{'Revenue (Rs/Acre)':<18}")
        print("-" * 80)
        
        for rank, row in df.iterrows():
            print(f"{rank:<6}{row['product_name']:<15}{row['avg_rate']:>14,.0f}{row['yield_per_acre']:>14.0f}{row['revenue_per_acre']:>18,.0f}")


def export_to_excel(results, output_path):
    """Export results to Excel file"""
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Summary sheet with top vegetable per month
        summary_data = []
        for month_name, df in results.items():
            if not df.empty:
                top = df.iloc[0]
                summary_data.append({
                    'Month': month_name,
                    'Top Vegetable': top['product_name'],
                    'Avg Rate (Rs/Quintal)': top['avg_rate'],
                    'Yield (Quintals/Acre)': top['yield_per_acre'],
                    'Revenue (Rs/Acre)': top['revenue_per_acre']
                })
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # Detailed sheet with top 5 per month
        all_data = []
        for month_name, df in results.items():
            df_copy = df.copy()
            df_copy.insert(0, 'Month', month_name)
            df_copy.insert(1, 'Rank', range(1, len(df_copy) + 1))
            all_data.append(df_copy)
        
        detailed_df = pd.concat(all_data, ignore_index=True)
        detailed_df.columns = ['Month', 'Rank', 'Vegetable', 'Avg Rate (Rs/Quintal)', 
                               'Yield (Quintals/Acre)', 'Revenue (Rs/Acre)']
        detailed_df.to_excel(writer, sheet_name='Detailed', index=False)
    
    print(f"\nResults exported to: {output_path}")


def main():
    # Load and process data
    veg_df = load_and_prepare_data('product_all.xlsx')
    print(f"Loaded {len(veg_df):,} vegetable records")
    print(f"Unique vegetables: {veg_df['product_name'].nunique()}")
    print(f"Date range: {veg_df['rate_date'].min().date()} to {veg_df['rate_date'].max().date()}")
    
    # Calculate profitability
    profitability_df = calculate_profitability(veg_df)
    
    # Get top 5 vegetables per month
    results = get_top_vegetables_by_month(profitability_df, top_n=5)
    
    # Print results
    print_results(results)
    
    # Export to Excel
    export_to_excel(results, 'vegetable_profitability_by_month.xlsx')


if __name__ == '__main__':
    main()
