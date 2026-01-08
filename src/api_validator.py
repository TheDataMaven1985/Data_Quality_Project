import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from src.quality_check import DataQualityChecker

# SETUP LOGGING
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class APIValidator:
    """Validates API data using DataQualityChecker before storing database"""

    def __init__(self):
        self.logger = logger
        self.results = {}

    def validate_weather_data(self, weather_data: Dict) -> Tuple[bool, Dict]:
        """Validate weather data structure and types"""
        self.logger.info("Validating weather data")
        
        # Check for empty dict
        if not weather_data:
            self.logger.error("Weather data is empty or None")
            return False, {'error': 'Empty or None weather data'}

        schema = {
            'city': str,
            'temperature': (int, float),
            'humidity': int,
            'pressure': int,
            'timestamp': (str, datetime),  # Accept both string and datetime
            'source': str
        }

        try:
            dq_checker = DataQualityChecker()
            is_valid, details = dq_checker.validate_dict(weather_data, schema)

            self.results['weather_data'] = details

            if is_valid:
                self.logger.info("Weather data validation passed.")
            else:
                self.logger.error(f"Weather data validation failed: {details}")

            return is_valid, details

        except Exception as e:
            self.logger.error(f"Error during weather data validation: {e}")
            return False, {'error': str(e)}

    def validate_cryptocurrencies_data(self, crypto_df: pd.DataFrame) -> Dict:
        """Validate cryptocurrencies DataFrame structure and types"""

        self.logger.info("Validating cryptocurrencies data")

        # Check for empty DataFrame
        if crypto_df is None or crypto_df.empty:
            error_result = {
                'validation_passed': False,
                'details': 'Empty or None DataFrame provided',
                'records_validated': 0
            }
            self.logger.error("Cryptocurrencies DataFrame is empty or None")
            return error_result

        expected_types = {
            'symbol': 'object',
            'name': 'object',
            'current_price': ('float64', 'int64'),
            'market_cap': ('int64', 'float64'),
            'market_cap_rank': ('int64', 'float64'),
            'trading_volume_24h': ('int64', 'float64'),
            'price_change_24h': ('float64', 'int64'),
        }


        try:
            checker = DataQualityChecker(crypto_df)
            result = checker.run_all_checks(expected_types)

            self.results['cryptocurrencies_data'] = result

            result['records_validated'] = len(crypto_df)

            if result.get('validation_passed', False):
                self.logger.info(f"Cryptocurrencies data validation passed. {len(crypto_df)} records validated.")
            else:
                self.logger.error(f"Cryptocurrencies data validation failed: {result.get('details', 'Unknown error')}")

            return result

        except Exception as e:
            self.logger.error(f"Error during cryptocurrencies validation: {e}")
            return {
                'validation_passed': False,
                'details': str(e),
                'records_validated': 0
            }

    def validate_posts_data(self, posts_df: pd.DataFrame) -> Dict:  # ⚠️ FIX #1: Changed 'df' to 'posts_df'
        """Validate posts DataFrame structure and types"""

        self.logger.info("Validating posts data")

        # Check for empty DataFrame
        if posts_df is None or posts_df.empty:
            error_result = {
                'validation_passed': False,
                'details': 'Empty or None DataFrame provided',
                'records_validated': 0
            }
            self.logger.error("Posts DataFrame is empty or None")
            return error_result

        expected_types = {
            'post_id': 'int64',
            'user_id': 'int64',
            'title': 'object',
            'body': 'object',
            'word_count': 'int64',

        }

        try:
            checker = DataQualityChecker(posts_df)
            result = checker.run_all_checks(expected_types)

            self.results['posts_data'] = result

            result['records_validated'] = len(posts_df)

            if result.get('validation_passed', False):
                self.logger.info(f"Posts data validation passed. {len(posts_df)} records validated.")
            else:
                self.logger.error(f"Posts data validation failed: {result.get('details', 'Unknown error')}")

            return result

        except Exception as e:
            self.logger.error(f"Error during posts validation: {e}")
            return {
                'validation_passed': False,
                'details': str(e),
                'records_validated': 0
            }

    def validate_all(self, weather_data: Optional[Dict] = None, 
                    crypto_df: Optional[pd.DataFrame] = None,
                    posts_df: Optional[pd.DataFrame] = None) -> Dict:
        """Validate all provided data sources"""
        all_results = {
            'timestamp': datetime.now().isoformat(),
            'validations': {},
            'overall_passed': True
        }

        if weather_data is not None:
            is_valid, details = self.validate_weather_data(weather_data)
            all_results['validations']['weather'] = {
                'passed': is_valid,
                'details': details
            }
            all_results['overall_passed'] &= is_valid

        if crypto_df is not None:
            result = self.validate_cryptocurrencies_data(crypto_df)
            all_results['validations']['cryptocurrencies'] = result
            all_results['overall_passed'] &= result.get('validation_passed', False)

        if posts_df is not None:
            result = self.validate_posts_data(posts_df)
            all_results['validations']['posts'] = result
            all_results['overall_passed'] &= result.get('validation_passed', False)

        self.logger.info(f"Overall validation result: {'PASSED' if all_results['overall_passed'] else 'FAILED'}")
        
        return all_results

    def get_validation_summary(self) -> Dict:
        """Get a summary of all validation results"""
        summary = {
            'total_validations': len(self.results),
            'passed': 0,
            'failed': 0,
            'details': self.results
        }

        for key, result in self.results.items():
            if isinstance(result, dict):
                if result.get('validation_passed', False):
                    summary['passed'] += 1
                else:
                    summary['failed'] += 1

        return summary

    def clear_results(self):
        """Clear stored validation results"""
        self.results = {}
        self.logger.info("Validation results cleared")


# Usage example
if __name__ == "__main__":
    validator = APIValidator()

    # Test with sample data
    posts_df = pd.DataFrame({
        'post_id': [1, 2, 3],
        'user_id': [10, 20, 30],
        'title': ['Title 1', 'Title 2', 'Title 3'],
        'body': ['Body text 1', 'Body text 2', 'Body text 3'],
        'word_count': [10, 15, 20],
        'timestamp': [datetime.now(), datetime.now(), datetime.now()]
    })

    result = validator.validate_posts_data(posts_df)
    print(f"Validation result: {result['validation_passed']}")