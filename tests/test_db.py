import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.api_data_store import APIDataStore


class TestAPIDataStore(unittest.TestCase):
    """Test cases for APIDataStore class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.store = APIDataStore()
    
    @patch('src.api_data_store.mysql.connector.connect')
    def test_connect_success(self, mock_connect):
        """Test successful database connection"""
        mock_connection = Mock()
        mock_connection.is_connected.return_value = True
        mock_connect.return_value = mock_connection
        
        result = self.store.connect()
        
        self.assertTrue(result)
        self.assertIsNotNone(self.store.connection)
    
    @patch('src.api_data_store.mysql.connector.connect')
    def test_connect_failure(self, mock_connect):
        """Test database connection failure"""
        mock_connect.side_effect = Exception("Connection failed")
        
        result = self.store.connect()
        
        self.assertFalse(result)
    
    def test_disconnect(self):
        """Test database disconnection"""
        mock_connection = Mock()
        mock_connection.is_connected.return_value = True
        self.store.connection = mock_connection
        
        self.store.disconnect()
        
        mock_connection.close.assert_called_once()
    
    @patch('src.api_data_store.mysql.connector.connect')
    def test_create_tables_not_connected(self, mock_connect):
        """Test create_tables when not connected"""
        self.store.connection = None
        
        result = self.store.create_tables()
        
        self.assertFalse(result)
    
    def test_store_cryptocurrency_empty_dataframe(self):
        """Test storing empty cryptocurrency DataFrame"""
        mock_connection = Mock()
        mock_connection.is_connected.return_value = True
        self.store.connection = mock_connection
        
        empty_df = pd.DataFrame()
        result = self.store.store_cryptocurrency(empty_df, True)
        
        self.assertEqual(result, 0)
    
    def test_store_cryptocurrency_success(self):
        """Test successful cryptocurrency data storage"""
        mock_connection = Mock()
        mock_connection.is_connected.return_value = True
        mock_cursor = Mock()
        mock_cursor.rowcount = 2
        mock_connection.cursor.return_value = mock_cursor
        self.store.connection = mock_connection
        
        test_df = pd.DataFrame({
            'symbol': ['BTC', 'ETH'],
            'name': ['Bitcoin', 'Ethereum'],
            'current_price': [50000.0, 3000.0],
            'market_cap': [1000000000000, 500000000000],
            'market_cap_rank': [1, 2],
            'trading_volume_24h': [50000000000, 30000000000],
            'price_change_24h': [2.5, -1.2],
            'timestamp': [datetime.now(), datetime.now()]
        })
        
        result = self.store.store_cryptocurrency(test_df, True)
        
        self.assertEqual(result, 2)
        mock_connection.commit.assert_called_once()

# Run tests
if __name__ == '__main__':
    unittest.main(verbosity=2)