import unittest
import pandas as pd
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.api_validator import APIValidator


class TestAPIValidator(unittest.TestCase):
    """Test cases for APIValidator class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.validator = APIValidator()
    
    def test_validate_cryptocurrencies_valid_data(self):
        """Test validation with valid cryptocurrency data"""
        valid_df = pd.DataFrame({
            'symbol': ['BTC', 'ETH'],
            'name': ['Bitcoin', 'Ethereum'],
            'current_price': [50000.0, 3000.0],
            'market_cap': [1000000000000, 500000000000],
            'market_cap_rank': [1, 2],
            'trading_volume_24h': [50000000000, 30000000000],
            'price_change_24h': [2.5, -1.2],
            'timestamp': [datetime.now(), datetime.now()]
        })
        
        result = self.validator.validate_cryptocurrencies_data(valid_df)
        
        self.assertTrue(result['validation_passed'])
        self.assertEqual(result['records_validated'], 2)
    
    def test_validate_cryptocurrencies_empty_dataframe(self):
        """Test validation with empty DataFrame"""
        empty_df = pd.DataFrame()
        
        result = self.validator.validate_cryptocurrencies_data(empty_df)
        
        self.assertFalse(result['validation_passed'])
        self.assertEqual(result['records_validated'], 0)
    
    def test_validate_cryptocurrencies_wrong_types(self):
        """Test validation with wrong data types"""
        invalid_df = pd.DataFrame({
            'symbol': ['BTC', 'ETH'],
            'name': ['Bitcoin', 'Ethereum'],
            'current_price': ['invalid', 'data'],  # Should be float
            'market_cap': [1000000000000, 500000000000],
            'market_cap_rank': [1, 2],
            'trading_volume_24h': [50000000000, 30000000000],
            'price_change_24h': [2.5, -1.2],
            'timestamp': [datetime.now(), datetime.now()]
        })
        
        result = self.validator.validate_cryptocurrencies_data(invalid_df)
        
        self.assertFalse(result['validation_passed'])
    
    def test_validate_posts_valid_data(self):
        """Test validation with valid posts data"""
        valid_df = pd.DataFrame({
            'post_id': [1, 2],
            'user_id': [10, 20],
            'title': ['Title 1', 'Title 2'],
            'body': ['Body text 1', 'Body text 2'],
            'word_count': [10, 15],
            'timestamp': [datetime.now(), datetime.now()]
        })
        
        result = self.validator.validate_posts_data(valid_df)
        
        self.assertTrue(result['validation_passed'])
        self.assertEqual(result['records_validated'], 2)
    
    def test_validate_weather_data_valid(self):
        """Test validation with valid weather data"""
        valid_weather = {
            'city': 'London',
            'temperature': 25.5,
            'humidity': 60,
            'pressure': 1013,
            'timestamp': datetime.now(),
            'source': 'weather_api'
        }
        
        is_valid, details = self.validator.validate_weather_data(valid_weather)
        
        self.assertTrue(is_valid)
    
    def test_validate_weather_data_empty(self):
        """Test validation with empty weather data"""
        is_valid, details = self.validator.validate_weather_data({})
        
        self.assertFalse(is_valid)
        self.assertIn('error', details)