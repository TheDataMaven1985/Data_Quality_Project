import pandas as pd
import logging
from typing import Dict, List, Optional
from dotenv import load_dotenv
from datetime import datetime
import os
import sys

# Load environment variables from .env file
load_dotenv()

# Import pipeline components
from src.api_fetcher import APIFetcher
from src.api_validator import APIValidator
from src.api_data_store import APIDataStore
from src.dashboard_gen import APIDashboard

# SETUP LOGGING
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Main Pipeline Script
def run_api_pipeline(fetch_weather: bool = False, weather_key: Optional[str] = None):
    """
    Complete data pipeline from APIs to database.
    """

    logger.info("STARTING API DATA PIPELINE")
    
    # Initialize
    fetcher = APIFetcher()
    validator = APIValidator()
    store = APIDataStore()
    
    # Connect to Database
    if not store.connect():
        logger.error("Failed to connect to database.")
        return
    
    if not store.create_tables():
        logger.error(" Cannot create tables.")
        store.disconnect()
        return
    
    # Track results
    pipeline_results = {
        'total_apis': 0,
        'successful_fetches': 0,
        'successful_validations': 0,
        'successful_stores': 0,
        'total_records_fetched': 0,
        'total_records_stored': 0,
        'start_time': datetime.now()
    }
    
    try:
        # Fetch Cryptocurrencies
        logger.info("\nFETCHING CRYPTOCURRENCY DATA")

        crypto_df = fetcher.fetch_cryptocurrencies(limit=20)

        if crypto_df is not None and not crypto_df.empty:
            pipeline_results['total_apis'] += 1
            pipeline_results['successful_fetches'] += 1
            pipeline_results['total_records_fetched'] += len(crypto_df)
            
            logger.info(f"Fetched {len(crypto_df)} cryptocurrency records")
            
            # Validate
            validation_result = validator.validate_cryptocurrencies_data(crypto_df)
            
            if validation_result['validation_passed']:
                pipeline_results['successful_validations'] += 1
                logger.info("Cryptocurrency data validation PASSED")
        
                stored_count = store.store_cryptocurrency(crypto_df, validation_passed=True)
                
                if stored_count > 0:
                    pipeline_results['successful_stores'] += 1
                    pipeline_results['total_records_stored'] += stored_count
                    logger.info(f"Stored {stored_count} cryptocurrency records in database")
                else:
                    logger.error("Failed to store cryptocurrency data")
            else:
                logger.error(f"Cryptocurrency validation FAILED: {validation_result.get('details', 'Unknown error')}")
        else:
            logger.error("Failed to fetch cryptocurrency data")

        # Fetch Posts
        logger.info("\nFETCHING POSTS DATA")

        posts_df = fetcher.fetch_posts(limit=50)

        if posts_df is not None and not posts_df.empty:
                pipeline_results['total_apis'] += 1
                pipeline_results['successful_fetches'] += 1
                pipeline_results['total_records_fetched'] += len(posts_df)
                
                logger.info(f"Fetched {len(posts_df)} posts records")
                
                # Validate
                validation_result = validator.validate_posts_data(posts_df)
                
                if validation_result['validation_passed']:
                    pipeline_results['successful_validations'] += 1
                    logger.info("Posts data validation PASSED")
                    
                    stored_count = store.store_posts(posts_df, validation_passed=True)
                    
                    if stored_count > 0:
                        pipeline_results['successful_stores'] += 1
                        pipeline_results['total_records_stored'] += stored_count
                        logger.info(f"Stored {stored_count} posts records in database")
                    else:
                        logger.error("Failed to store posts data")
                else:
                    logger.error(f"Posts validation FAILED: {validation_result.get('details', 'Unknown error')}")
        else:
            logger.error("Failed to fetch posts data")
        
        # Fetch Weather 
        if fetch_weather:
            logger.info("FETCHING WEATHER DATA")
                
            if not weather_key:
                logger.warning("Weather API key not provided, skipping weather data")
            else:
                weather_data = fetcher.fetch_weather('London')
                    
                if weather_data:
                    pipeline_results['total_apis'] += 1
                    pipeline_results['successful_fetches'] += 1
                    pipeline_results['total_records_fetched'] += 1
                        
                    logger.info(f"Fetched weather data for {weather_data.get('city', 'Unknown')}")
                        
                    # Validate
                    is_valid, details = validator.validate_weather_data(weather_data)
                        
                    if is_valid:
                        pipeline_results['successful_validations'] += 1
                        logger.info("Weather data validation PASSED")
                            
                        # Store weather summary
                        weather_summary = {
                            'api_name': 'OpenWeatherMap API',
                            'status': 'success',
                            'records_fetched': 1,
                            'records_validated': 1,
                            'records_stored': 1,
                            'errors_found': 0,
                            'timestamp': datetime.now()
                        }
                            
                        stored_count = store.store_weather_data(weather_summary, validation_passed=True)
                            
                        if stored_count > 0:
                            pipeline_results['successful_stores'] += 1
                            pipeline_results['total_records_stored'] += stored_count
                            logger.info(f"Stored weather data summary in database")
                        else:
                            logger.error("Failed to store weather data")
                    else:
                        logger.error(f"Weather validation FAILED: {details}")
                else:
                    logger.error("Failed to fetch weather data")
    except Exception as e:
        logger.error(f"An error occurred during the pipeline: {e}", exc_info=True)
    finally:
        store.disconnect()
    
    # Print summary
    duration = (datetime.now() - pipeline_results['start_time']).total_seconds()
    
    logger.info("PIPELINE SUMMARY")
    logger.info(f"APIs called: {pipeline_results['total_apis']}")
    logger.info(f"Successful fetches: {pipeline_results['successful_fetches']}")
    logger.info(f"Successful validations: {pipeline_results['successful_validations']}")
    logger.info(f"Successful stores: {pipeline_results['successful_stores']}")
    logger.info(f"Total records fetched: {pipeline_results['total_records_fetched']}")
    logger.info(f"Total records stored: {pipeline_results['total_records_stored']}")
    logger.info(f"Duration: {duration:.2f} seconds")
    logger.info("PIPELINE COMPLETE")

    # Dashboard Generation
    if pipeline_results['successful_stores'] > 0:
        logger.info("\nGENERATING DASHBOARD...")
        try:
            with APIDashboard() as dashboard:
                dashboard.generate_html()
                logger.info("Dashboard created: dashboard.html")
                logger.info(f"Open: file://{os.path.abspath('dashboard.html')}")
        except Exception as e:
            logger.error(f"Failed to generate dashboard: {e}")

if __name__ == "__main__":
    # Run pipeline
    run_api_pipeline(
        fetch_weather=False,  # Set to True if you have OpenWeather API key
        weather_key=os.getenv('WEATHER_API_KEY')
    )