import unittest
import pandas as pd
from vegetable_analysis import (
    VegetableAnalysisServiceBuilder,
    MarathiVegetableFilter,
    PerAcreProductionProvider,
    VegetableDataCleaner,
    PriceRankingStrategy
)


class TestVegetableAnalysis(unittest.TestCase):
    
    def test_service_runs_without_error(self):
        service = (VegetableAnalysisServiceBuilder()
                   .with_filepath('/tmp/inputs/product_all.xlsx')
                   .build())
        report = service.analyze()
        self.assertIsInstance(report, pd.DataFrame)
        self.assertGreater(len(report), 0)
    
    def test_vegetable_filter_returns_vegetables(self):
        filter = MarathiVegetableFilter()
        names = filter.get_vegetable_names()
        self.assertIn('कांदा', names)
        self.assertEqual(filter.get_english_name('कांदा'), 'Onion')
    
    def test_production_provider_returns_data(self):
        provider = PerAcreProductionProvider()
        self.assertEqual(provider.get_production('Tomato'), 150)
        self.assertIsNone(provider.get_production('NonExistent'))
    
    def test_price_ranking_returns_top_n(self):
        df = pd.DataFrame({
            'product_name': ['A', 'B', 'C', 'D'],
            'avg_price': [100, 200, 150, 50],
            'total_quantity': [10, 20, 30, 40],
            'per_acre_production': [5, 6, 7, 8]
        })
        strategy = PriceRankingStrategy()
        result = strategy.rank(df, 2)
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]['avg_price'], 200)


if __name__ == '__main__':
    unittest.main()
