import pandas as pd
import logging
import os
from dotenv import load_dotenv
import mysql.connector
from typing import Dict, List, Optional
from datetime import datetime

# Load .env
load_dotenv()

# SETUP LOGGING
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# DATABASE class
class APIDataStore:
    """Store API data in MySQL  
    Creates Table Automatically if they don't exist """

    def __init__(self):
        """Initialize Database Connection"""
        self.logger = logger
        self.connection = None

    def connect(self) -> bool:
        """Establish Database Connection"""
        try:
            self.logger.info("Connecting to the database...")

            self.connection = mysql.connector.connect(
                host = os.getenv('DB_HOST', 'localhost'),
                user = os.getenv('DB_USER'),
                password = os.getenv('DB_PASSWORD', ''),
                database = os.getenv('DB_NAME')
            )

            if self.connection.is_connected():
                self.logger.info("Database connection established.")
                return True
            return False   # return for failed connection

        except Exception as e:
            self.logger.error(f"Error connecting to database: {e}")
            return False

    # Disconnect method
    def disconnect(self):  
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.logger.info("Database connection closed.")
    
    # Create Tables
    def create_tables(self) -> bool:
        """Create Required Tables if they don't exist"""

        # Check connection
        if not self.connection or not self.connection.is_connected(): 
            self.logger.error("Not connected to database. Call connect() first.")
            return False

        try:
            cursor = self.connection.cursor()

            # Table 1: Cryptocurrencies
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cryptocurrency_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    name VARCHAR(100),
                    current_price DECIMAL(20, 8),
                    market_cap BIGINT,
                    market_cap_rank INT,
                    trading_volume_24h BIGINT,
                    price_change_24h DECIMAL(10, 4),
                    timestamp DATETIME,
                    validation_passed BOOLEAN,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_symbol (symbol),
                    INDEX idx_timestamp (timestamp)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            # Table 2: Posts Data
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS posts_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    post_id INT NOT NULL UNIQUE,
                    user_id INT,
                    title VARCHAR(500),
                    body LONGTEXT,
                    word_count INT,
                    timestamp DATETIME,
                    validation_passed BOOLEAN,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_user_id (user_id),
                    INDEX idx_timestamp (timestamp),
                    INDEX idx_post_id (post_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            # Table 3: Weather Data
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS weather_data(
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    api_name VARCHAR(100),
                    status VARCHAR(20),  -- 'success' or 'failed'
                    records_fetched INT DEFAULT 0,
                    records_validated INT DEFAULT 0,
                    records_stored INT DEFAULT 0,
                    errors_found INT DEFAULT 0,
                    timestamp DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_api_name (api_name),
                    INDEX idx_timestamp (timestamp)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            self.connection.commit()
            cursor.close()
            self.logger.info("Tables created or already exist.")
            return True

        except Exception as e:
            self.logger.error(f"Error creating tables: {e}")
            return False

    # Insert Data
    def store_cryptocurrency(self, crypto_df: pd.DataFrame, validation_passed: bool) -> int:
        """Store Cryptocurrency Data into Database"""

        # Check for Database Connection
        if not self.connection or not self.connection.is_connected():
            self.logger.error("Not connected to database.")
            return 0

        # Check for empty DataFrame
        if crypto_df.empty:
            self.logger.warning("Empty DataFrame provided for cryptocurrency data.")
            return 0

        try:
            cursor = self.connection.cursor()

            insert_query = """
                INSERT INTO cryptocurrency_data (
                    symbol, name, current_price, market_cap, market_cap_rank,
                    trading_volume_24h, price_change_24h, timestamp, validation_passed
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                current_price = VALUES(current_price),
                market_cap = VALUES(market_cap),
                market_cap_rank = VALUES(market_cap_rank),
                trading_volume_24h = VALUES(trading_volume_24h),
                price_change_24h = VALUES(price_change_24h),
                timestamp = VALUES(timestamp),
                validation_passed = VALUES(validation_passed)
            """

            records_to_insert = []
            for _, row in crypto_df.iterrows():
                timestamp = row.get('timestamp', datetime.now())
                # Convert datetime/Timestamp to string format MySQL understands
                if isinstance(timestamp, (datetime, pd.Timestamp)):
                    timestamp = str(timestamp)[:19]  # Extract 'YYYY-MM-DD HH:MM:SS'
                record = (
                    str(row.get('symbol', '')),
                    str(row.get('name', '')),
                    float(row.get('current_price', 0)) if pd.notna(row.get('current_price')) else None,
                    int(row.get('market_cap', 0)) if pd.notna(row.get('market_cap')) else None,
                    int(row.get('market_cap_rank', 0)) if pd.notna(row.get('market_cap_rank')) else None,
                    int(row.get('trading_volume_24h', 0)) if pd.notna(row.get('trading_volume_24h')) else None,
                    float(row.get('price_change_24h', 0)) if pd.notna(row.get('price_change_24h')) else None,
                    timestamp,
                    validation_passed
                )
                records_to_insert.append(record)

            cursor.executemany(insert_query, records_to_insert)
            self.connection.commit()
            inserted_count = cursor.rowcount
            self.logger.info(f"Inserted {inserted_count} cryptocurrency records.")
            cursor.close()
            return inserted_count

        except Exception as e:
            self.logger.error(f"Error inserting cryptocurrency data: {e}")
            self.connection.rollback() # Rollback on errors
            return 0 

    def store_posts(self, posts_df: pd.DataFrame, validation_passed: bool) -> int:
        """Store Posts Data into Database"""

        # Check Database Connections
        if not self.connection or not self.connection.is_connected():
            self.logger.error("Not connected to database.")
            return 0

        # Check for Empty DataFrame
        if posts_df.empty:
            self.logger.warning("Empty DataFrame provided for posts data.")
            return 0

        try:
            cursor = self.connection.cursor()

            insert_query = """
                INSERT INTO posts_data (
                    post_id, user_id, title, body, word_count, timestamp, validation_passed
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE  
                    title = VALUES(title),
                    body = VALUES(body),
                    word_count = VALUES(word_count)
            """

            records_to_insert = []
            for _, row in posts_df.iterrows():
                timestamp = row.get('timestamp', datetime.now())
                # Convert datetime/Timestamp to string format MySQL understands
                if isinstance(timestamp, (datetime, pd.Timestamp)):
                    timestamp = str(timestamp)[:19]  # Extract 'YYYY-MM-DD HH:MM:SS'
                record = (
                    int(row.get('post_id', 0)),
                    int(row.get('user_id', 0)) if pd.notna(row.get('user_id')) else None,
                    str(row.get('title', ''))[:500],
                    str(row.get('body', '')),
                    int(row.get('word_count', 0)) if pd.notna(row.get('word_count')) else 0,
                    timestamp,
                    validation_passed
                )
                records_to_insert.append(record)

            cursor.executemany(insert_query, records_to_insert)
            self.connection.commit()
            inserted_count = cursor.rowcount
            self.logger.info(f"Inserted {inserted_count} posts records.")
            cursor.close()
            return inserted_count

        except Exception as e:
            self.logger.error(f"Error inserting posts data: {e}")
            self.logger.debug(f"Failed records details: {records_to_insert[:1] if records_to_insert else 'No records'}")
            self.connection.rollback()  # Rollback on errors
            return 0

    def store_weather_data(self, weather_summary: Dict) -> int:
        """Store Weather Data Summary into Database"""

        # Check Database Connection
        if not self.connection or not self.connection.is_connected():
            self.logger.error("Not connected to database.")
            return 0

        try:
            cursor = self.connection.cursor()

            insert_query = """
                INSERT INTO weather_data (
                    api_name, status, records_fetched, records_validated,
                    records_stored, errors_found, timestamp
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            record = (
                weather_summary.get('api_name', 'OpenWeatherMap API'),
                weather_summary.get('status', 'failed'),
                int(weather_summary.get('records_fetched', 0)),
                int(weather_summary.get('records_validated', 0)),
                int(weather_summary.get('records_stored', 0)),
                int(weather_summary.get('errors_found', 0)),
                weather_summary.get('timestamp', datetime.now())
            )

            cursor.execute(insert_query, record)
            self.connection.commit()
            inserted_count = cursor.rowcount
            self.logger.info(f"Inserted weather data summary record.")
            cursor.close()
            return inserted_count

        except Exception as e:
            self.logger.error(f"Error inserting weather data summary: {e}")
            self.connection.rollback()  # Rollback on errors
            return 0

    # After inserting, verify immediately:
    def verify_insert(self, table_name: str):
        """Verify data was inserted into specified table"""
        try:
            cursor = self.connection.cursor()
            
            # Count records
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            self.logger.info(f"Total records in {table_name}: {count}")
            
            # Show recent records
            cursor.execute(f"SELECT * FROM {table_name} ORDER BY id DESC LIMIT 5")
            recent = cursor.fetchall()
            self.logger.info(f"Recent records from {table_name}: {recent}")
            
            cursor.close()
            return count
            
        except Exception as e:
            self.logger.error(f"Error verifying insert: {e}")
            return 0

    def get_table_stats(self):
        """Get statistics for all tables"""
        try:
            cursor = self.connection.cursor(dictionary=True)
            
            tables = ['cryptocurrency_data', 'posts_data', 'weather_data']
            stats = {}
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                result = cursor.fetchone()
                stats[table] = result['count']
                self.logger.info(f"{table}: {result['count']} records")
            
            cursor.close()
            return stats
            
        except Exception as e:
            self.logger.error(f"Error getting table stats: {e}")
            return {}

    def __enter__(self): 
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()


if __name__ == '__main__':
    with APIDataStore() as db:
        if db.create_tables():
            print("Database tables created successfully!.")

            test_crypto_df = pd.DataFrame({
                'symbol': ['BTC', 'ETH'],
                'name': ['Bitcoin', 'Ethereum'],
                'current_price': [45000.50, 3200.75],
                'market_cap': [850000000000, 380000000000],
                'market_cap_rank': [1, 2],
                'trading_volume_24h': [25000000000, 15000000000],
                'price_change_24h': [2.5, -1.2],
                'timestamp': [datetime.now(), datetime.now()]
            })
            
            inserted = db.store_cryptocurrency(test_crypto_df, validation_passed=True)
            print(f"Inserted {inserted} cryptocurrency records")

            db.verify_insert('cryptocurrency_data')

            stats = db.get_table_stats()
            print(f"\n Database Statistics: {stats}")