import pandas as pd
import requests
import json
import time
import logging
from typing import Dict, List, Optional
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables from .env file 
load_dotenv()

# SETUP LOGGING
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# API CONFIGURATION
API_CONFIG = {
    'weather': {
        'name': 'Weather API',
        'base_url': 'https://api.openweathermap.org/data/2.5/weather',
        'free': True,
        'key_required': True,
        'sign_up': 'https://openweathermap.org/api',
        'rate_limit': 60, # calls per minute
        'description': 'Provides current weather data for any location including over 200,000 cities.'
    },

    'crypto': {
        'name': 'Coingecko API',
        'base_url': 'https://api.coingecko.com/api/v3',
        'free': True,
        'key_required': False,
        'sign_up': 'Not Required',
        'rate_limit': 10,  # calls per minute
        'description': 'Offers cryptocurrency data including price, market capitalization, and trading volume for thousands of cryptocurrencies.'
    },

    'posts': {
        'name': 'DummyJSON API',
        'base_url': 'https://dummyjson.com/posts',
        'free': True,
        'key_required': False,
        'sign_up': 'Not Required',
        'rate_limit': None,  # Unlimited for testing purposes
        'description': 'Test data API with users, posts, comments.'
    }
}

class APIFetcher:

    def __init__(self, timeout: int = 30):
        self.session = requests.Session()
        self.timeout = timeout
        self.logger = logger

        # Load API keys from environment variables
        self.weather_api_key = os.getenv("WEATHER_API_KEY")
        if not self.weather_api_key:
            self.logger.warning("WEATHER_API_KEY not found in environment variables")

    def fetch_weather(self, city: str) -> Optional[Dict]:
        """Fetch current weather data for a given city.
        Returns Weather data including temperature, humidity, wind speed.
        """

        if not self.weather_api_key:
            raise RuntimeError("WEATHER_API_KEY is not set")

        try:
            self.logger.info(f"Fetching weather data for city: {city}")

            params = {
                'q': city,
                'appid': self.weather_api_key,
                'units': 'metric'   # Use celsius
            }

            response = self.session.get(
                API_CONFIG['weather']['base_url'],
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()

            data = response.json()

            # Extract relevant fields
            # Just one dictionary
            weather_data = {
                    'city': city,
                    'temperature': data['main']['temp'],
                    'humidity': data['main']['humidity'],
                    'wind_speed': data['wind']['speed'],
                    'description': data['weather'][0]['description'],
                    'pressure': data['main']['pressure'],
                    'timestamp': datetime.now(),
                    'source': 'weather_api'
            }

            self.logger.info(f"Successfully fetched weather data for city: {city}")
            return weather_data

        except requests.exceptions.HTTPError as e:
            if response.status_code == 401:
                self.logger.error(f"Authentication failed: Invalid API key for weather API. Please check your OpenWeatherMap API key.")
            else:
                self.logger.error(f"HTTP Error fetching weather data: {e}")
            return None
        except requests.RequestException as e:
            self.logger.error(f"Error fetching weather data: {e}")
            return None

    def fetch_cryptocurrencies(self, limit: int = 30) -> Optional[pd.DataFrame]:
        """Fetch current cryptocurrency data from Coingecko.
        Returns list of cryptocurrencies with their prices and market caps.
        """

        try:
            self.logger.info("Fetching cryptocurrency data")

            url = f"{API_CONFIG['crypto']['base_url']}/coins/markets"
            params = {
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'per_page': limit,
                'page': 1,
                'sparkline': 'false'
            }

            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()

            data = response.json()

            # Transform data into DataFrame
            crypto_data = []
            for coin in data:
                crypto_data.append({
                    'symbol': coin.get('symbol', '').upper(),
                    'name': coin.get('name', ''),
                    'current_price': coin.get('current_price', 0),
                    'market_cap': coin.get('market_cap', 0),
                    'market_cap_rank': coin.get('market_cap_rank', 0),
                    'trading_volume_24h': coin.get('total_volume', 0),
                    'price_change_24h': coin.get('price_change_percentage_24h', 0),
                    'timestamp': datetime.now(),
                    'source_api': 'Coingecko API'
                })

            crypto_df = pd.DataFrame(crypto_data)
            self.logger.info("Successfully fetched cryptocurrency data")
            return crypto_df

        except requests.RequestException as e:
            self.logger.error(f"Error fetching cryptocurrency data: {e}")
            return None

    def fetch_posts(self, limit: int = 10) -> Optional[pd.DataFrame]:
        """Fetch posts from DummyJSON API.
        Returns Sample blog posts with user data.
        """

        try:
            self.logger.info("Fetching posts data")

            url = f"{API_CONFIG['posts']['base_url']}"
            params = {"_limit": limit}

            response = self.session.get(
                url,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()

            data = response.json()
            posts = data["posts"]

            # Transform data into DataFrame
            posts_data = []
            for post in posts:
                posts_data.append({
                    'post_id': post.get('id', 0),
                    'user_id': post.get('userId', 0),
                    'title': post.get('title', ''),
                    'body': post.get('body', ''),
                    'word_count': len(post.get('body', '').split()),
                    'timestamp': datetime.now(),
                    'source_api': 'DummyJSON API'
                })

            posts_df = pd.DataFrame(posts_data)
            self.logger.info("Successfully fetched posts data")
            return posts_df

        except requests.RequestException as e:
            self.logger.error(f"Error fetching posts data: {e}")
            return None

# Main section
if __name__ == "__main__":
    print("\n" + '='*70)
    print("STARTING API FETCHER")
    print('='*70 + "\n")

    # Initialize APIFetcher
    api_fetcher = APIFetcher()

    # Fetch Cryptocurrency Data
    print("Fetching Cryptocurrency Data...")
    try:
        crypto_df = api_fetcher.fetch_cryptocurrencies(limit=5)
        if crypto_df is not None and len(crypto_df) > 0:
            print("Cryptocurrency Data Fetched Successfully:")
            print(crypto_df[['symbol', 'current_price', 'market_cap_rank']].to_string())
            print("\nTotal Cryptocurrencies Fetched:", len(crypto_df))
        else:
            print("Failed to fetch cryptocurrency data.")

    except Exception as e:
        print(f"An error occurred while fetching cryptocurrency data: {e}")

    # Fetch Posts Data
    print("\n" + '='*70)
    print("Fetching Posts Data...")
    print('='*70 + "\n")
    try:
        posts_df = api_fetcher.fetch_posts(limit=5)
        if posts_df is not None and len(posts_df) > 0:
            print("Posts Data Fetched Successfully:")
            print(posts_df[['post_id', 'user_id', 'title', 'word_count']].to_string())
            print("\nTotal Posts Fetched:", len(posts_df))
        else:
            print("Failed to fetch posts data.")

    except Exception as e:
        print(f"An error occurred while fetching posts data: {e}")

    # Note: Weather API requires an API key, so it's not included in this main section.
    print("\n" + '='*70)
    print("Fetching Weather API")
    print('='*70 + "\n")

    try:
        weather_data = api_fetcher.fetch_weather(city='New York')

        if weather_data is not None:
            print("Weather Data Fetched Successfully:")
            print(weather_data)
        else:
            print("Failed to fetch weather data.")
    
    except Exception as e:
        print(f"An error occurred while fetching weather data: {e}")

    print("\n" + '='*70)
    print("API FETCHER COMPLETED")
    print('='*70 + "\n")