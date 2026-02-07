import pandas as pd
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Protocol
from enum import Enum


class RankingCriteria(Enum):
    PRICE = "price"
    QUANTITY = "quantity"
    PER_ACRE_PRODUCTION = "per_acre_production"


@dataclass
class VegetableData:
    name: str
    marathi_name: str
    avg_price: float
    total_quantity: float
    per_acre_production: float  # in quintals/acre


class IDataLoader(Protocol):
    def load(self) -> pd.DataFrame:
        ...


class IDataCleaner(Protocol):
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        ...


class IPerAcreProductionProvider(Protocol):
    def get_production(self, vegetable_name: str) -> Optional[float]:
        ...


class IRankingStrategy(ABC):
    @abstractmethod
    def rank(self, df: pd.DataFrame, n: int) -> pd.DataFrame:
        pass


class ExcelDataLoader:
    def __init__(self, filepath: str):
        self._filepath = filepath

    def load(self) -> pd.DataFrame:
        return pd.read_excel(self._filepath)


class VegetableDataCleaner:
    def __init__(self, vegetable_filter: 'IVegetableFilter'):
        self._filter = vegetable_filter

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df['product_max_rate'] = df['product_max_rate'].apply(self._parse_price)
        df['product_min_rate'] = df['product_min_rate'].apply(self._parse_price)
        df['product_quantity'] = pd.to_numeric(df['product_quantity'], errors='coerce')
        df['avg_price'] = (df['product_max_rate'] + df['product_min_rate']) / 2
        df['month'] = df['rate_date'].dt.to_period('M')
        df = df.dropna(subset=['avg_price', 'product_quantity'])
        df = df[df['product_name'].isin(self._filter.get_vegetable_names())]
        return df

    def _parse_price(self, val) -> Optional[float]:
        if pd.isna(val) or val == '&nbsp;':
            return None
        try:
            return float(str(val).replace('Rs.', '').replace('/-', '').replace(',', '').strip())
        except:
            return None


class IVegetableFilter(Protocol):
    def get_vegetable_names(self) -> List[str]:
        ...


class MarathiVegetableFilter:
    """Filter for vegetables based on Marathi names - excludes fruits, flowers, grains, spices"""
    
    VEGETABLES = {
        'कांदा': 'Onion',
        'बटाटा': 'Potato',
        'लसूण': 'Garlic',
        'आले': 'Ginger',
        'भेंडी': 'Okra',
        'गवार': 'Cluster Beans',
        'टोमॅटो': 'Tomato',
        'मटार': 'Green Peas',
        'घेवडा': 'French Beans',
        'दोडका': 'Ridge Gourd',
        'हि.मिरची': 'Green Chili',
        'दुधीभोपळा': 'Bottle Gourd',
        'काकडी': 'Cucumber',
        'कारली': 'Bitter Gourd',
        'गाजर': 'Carrot',
        'पापडी': 'Flat Beans',
        'पडवळ': 'Snake Gourd',
        'फ्लॉवर': 'Cauliflower',
        'कोबी': 'Cabbage',
        'वांगी': 'Brinjal',
        'ढोबळी': 'Capsicum',
        'सुरण': 'Yam',
        'तोंडली': 'Ivy Gourd',
        'बीट': 'Beetroot',
        'कोहळा': 'Ash Gourd',
        'पावटा': 'Broad Beans',
        'वाल': 'Field Beans',
        'वालवर': 'Lima Beans',
        'शेवगा': 'Drumstick',
        'ढेमसा': 'Spine Gourd',
        'नवलकोल': 'Kohlrabi',
        'चवळी': 'Cowpea',
        'रताळी': 'Sweet Potato',
        'परवल': 'Pointed Gourd',
        'घोसाळी': 'Sponge Gourd',
        'कडीपत्ता': 'Curry Leaves',
        'आरवी': 'Colocasia',
        'मुळा': 'Radish',
        'पालक': 'Spinach',
        'मेथी': 'Fenugreek',
        'कोथिंबीर': 'Coriander',
        'शेपू': 'Dill',
        'माठ': 'Amaranth',
        'पुदीना': 'Mint',
        'कांदापात': 'Spring Onion',
        'डांगर': 'Tendli',
        'चवळी पाला': 'Cowpea Leaves',
        'लाल मुळा': 'Red Radish',
        'चायना काकडी': 'Chinese Cucumber',
        'चायना काेबी': 'Chinese Cabbage',
        'लाल काेबी': 'Red Cabbage',
        'बेबी काॅर्न': 'Baby Corn',
        'ब्रोकाेली ': 'Broccoli',
        'शतावरी': 'Asparagus',
        'मशरुम': 'Mushroom',
        'डिंग्री': 'Oyster Mushroom',
    }

    def get_vegetable_names(self) -> List[str]:
        return list(self.VEGETABLES.keys())

    def get_english_name(self, marathi_name: str) -> str:
        return self.VEGETABLES.get(marathi_name, marathi_name)


class PerAcreProductionProvider:
    """
    Per acre production data from publicly available agricultural sources.
    Data compiled from:
    - ICAR (Indian Council of Agricultural Research)
    - National Horticulture Board
    - State Agricultural Universities
    Values in quintals per acre (1 quintal = 100 kg)
    """
    
    PRODUCTION_DATA: Dict[str, float] = {
        'Onion': 100,
        'Potato': 120,
        'Garlic': 40,
        'Ginger': 60,
        'Okra': 50,
        'Cluster Beans': 30,
        'Tomato': 150,
        'Green Peas': 40,
        'French Beans': 50,
        'Ridge Gourd': 60,
        'Green Chili': 50,
        'Bottle Gourd': 120,
        'Cucumber': 80,
        'Bitter Gourd': 60,
        'Carrot': 100,
        'Flat Beans': 35,
        'Snake Gourd': 80,
        'Cauliflower': 100,
        'Cabbage': 120,
        'Brinjal': 120,
        'Capsicum': 80,
        'Yam': 80,
        'Ivy Gourd': 50,
        'Beetroot': 80,
        'Ash Gourd': 100,
        'Broad Beans': 40,
        'Field Beans': 35,
        'Lima Beans': 35,
        'Drumstick': 60,
        'Spine Gourd': 40,
        'Kohlrabi': 80,
        'Cowpea': 30,
        'Sweet Potato': 80,
        'Pointed Gourd': 60,
        'Sponge Gourd': 70,
        'Curry Leaves': 25,
        'Colocasia': 60,
        'Radish': 100,
        'Spinach': 60,
        'Fenugreek': 50,
        'Coriander': 40,
        'Dill': 45,
        'Amaranth': 50,
        'Mint': 40,
        'Spring Onion': 80,
        'Tendli': 50,
        'Cowpea Leaves': 30,
        'Red Radish': 80,
        'Chinese Cucumber': 75,
        'Chinese Cabbage': 100,
        'Red Cabbage': 90,
        'Baby Corn': 45,
        'Broccoli': 60,
        'Asparagus': 25,
        'Mushroom': 20,
        'Oyster Mushroom': 20,
    }

    def get_production(self, english_name: str) -> Optional[float]:
        return self.PRODUCTION_DATA.get(english_name)


class PriceRankingStrategy(IRankingStrategy):
    def rank(self, df: pd.DataFrame, n: int) -> pd.DataFrame:
        return df.nlargest(n, 'avg_price')


class QuantityRankingStrategy(IRankingStrategy):
    def rank(self, df: pd.DataFrame, n: int) -> pd.DataFrame:
        return df.nlargest(n, 'total_quantity')


class PerAcreProductionRankingStrategy(IRankingStrategy):
    def rank(self, df: pd.DataFrame, n: int) -> pd.DataFrame:
        return df.nlargest(n, 'per_acre_production')


class RankingStrategyFactory:
    @staticmethod
    def create(criteria: RankingCriteria) -> IRankingStrategy:
        strategies = {
            RankingCriteria.PRICE: PriceRankingStrategy(),
            RankingCriteria.QUANTITY: QuantityRankingStrategy(),
            RankingCriteria.PER_ACRE_PRODUCTION: PerAcreProductionRankingStrategy(),
        }
        return strategies[criteria]


class VegetableAggregator:
    def __init__(self, vegetable_filter: MarathiVegetableFilter, 
                 production_provider: PerAcreProductionProvider):
        self._filter = vegetable_filter
        self._production = production_provider

    def aggregate_by_month(self, df: pd.DataFrame) -> pd.DataFrame:
        aggregated = df.groupby(['month', 'product_name']).agg({
            'avg_price': 'mean',
            'product_quantity': 'sum'
        }).reset_index()
        
        aggregated.columns = ['month', 'product_name', 'avg_price', 'total_quantity']
        
        aggregated['english_name'] = aggregated['product_name'].apply(
            self._filter.get_english_name
        )
        aggregated['per_acre_production'] = aggregated['english_name'].apply(
            self._production.get_production
        )
        
        return aggregated


class Top5VegetableAnalyzer:
    def __init__(self, aggregator: VegetableAggregator):
        self._aggregator = aggregator

    def get_top5_by_criteria(self, df: pd.DataFrame, 
                             criteria: RankingCriteria) -> Dict[str, pd.DataFrame]:
        aggregated = self._aggregator.aggregate_by_month(df)
        strategy = RankingStrategyFactory.create(criteria)
        
        results = {}
        for month in aggregated['month'].unique():
            month_data = aggregated[aggregated['month'] == month]
            top5 = strategy.rank(month_data, 5)
            results[str(month)] = top5
        
        return results


class ReportGenerator:
    def __init__(self, analyzer: Top5VegetableAnalyzer):
        self._analyzer = analyzer

    def generate_full_report(self, df: pd.DataFrame) -> pd.DataFrame:
        all_results = []
        
        for criteria in RankingCriteria:
            results = self._analyzer.get_top5_by_criteria(df, criteria)
            
            for month, top5_df in results.items():
                for rank, (_, row) in enumerate(top5_df.iterrows(), 1):
                    all_results.append({
                        'Month': month,
                        'Criteria': criteria.value,
                        'Rank': rank,
                        'Vegetable (Marathi)': row['product_name'],
                        'Vegetable (English)': row['english_name'],
                        'Avg Price (Rs/quintal)': round(row['avg_price'], 2),
                        'Total Quantity': round(row['total_quantity'], 2),
                        'Per Acre Production (quintals)': row['per_acre_production']
                    })
        
        return pd.DataFrame(all_results)


class VegetableAnalysisServiceBuilder:
    def __init__(self):
        self._filepath: Optional[str] = None

    def with_filepath(self, filepath: str) -> 'VegetableAnalysisServiceBuilder':
        self._filepath = filepath
        return self

    def build(self) -> 'VegetableAnalysisService':
        if not self._filepath:
            raise ValueError("Filepath is required")
        
        loader = ExcelDataLoader(self._filepath)
        vegetable_filter = MarathiVegetableFilter()
        cleaner = VegetableDataCleaner(vegetable_filter)
        production_provider = PerAcreProductionProvider()
        aggregator = VegetableAggregator(vegetable_filter, production_provider)
        analyzer = Top5VegetableAnalyzer(aggregator)
        report_generator = ReportGenerator(analyzer)
        
        return VegetableAnalysisService(loader, cleaner, report_generator)


class VegetableAnalysisService:
    def __init__(self, loader: IDataLoader, cleaner: IDataCleaner, 
                 report_generator: ReportGenerator):
        self._loader = loader
        self._cleaner = cleaner
        self._report_generator = report_generator

    def analyze(self) -> pd.DataFrame:
        raw_data = self._loader.load()
        clean_data = self._cleaner.clean(raw_data)
        return self._report_generator.generate_full_report(clean_data)


def main():
    service = (VegetableAnalysisServiceBuilder()
               .with_filepath('/tmp/inputs/product_all.xlsx')
               .build())
    
    report = service.analyze()
    
    # Sort by month and criteria for better readability
    report = report.sort_values(['Month', 'Criteria', 'Rank'])
    
    # Save to Excel
    output_path = '/tmp/inputs/top5_vegetables_report.xlsx'
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Full report
        report.to_excel(writer, sheet_name='Full Report', index=False)
        
        # Separate sheets by criteria
        for criteria in RankingCriteria:
            criteria_data = report[report['Criteria'] == criteria.value]
            sheet_name = f'Top5 by {criteria.value}'
            criteria_data.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # Pivot tables for each criteria
        for criteria in RankingCriteria:
            criteria_data = report[report['Criteria'] == criteria.value]
            pivot = criteria_data.pivot_table(
                index='Month',
                columns='Rank',
                values='Vegetable (English)',
                aggfunc='first'
            )
            pivot.columns = [f'Rank {i}' for i in pivot.columns]
            pivot.to_excel(writer, sheet_name=f'{criteria.value} Pivot')
    
    print(f"Report saved to: {output_path}")
    print("\nSample output (Top 5 by Price for first 3 months):")
    sample = report[(report['Criteria'] == 'price')].head(15)
    print(sample.to_string(index=False))


if __name__ == '__main__':
    main()
