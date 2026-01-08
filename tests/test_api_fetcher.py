from api_data_store import APIDataStore
import pandas as pd
from datetime import datetime

# Connect and test
db = APIDataStore()
if db.connect():
    # Create tables
    db.create_tables()
    
    # Insert test data
    test_df = pd.DataFrame({
        'symbol': ['BTC'],
        'name': ['Bitcoin'],
        'current_price': [45000.0],
        'market_cap': [850000000000],
        'market_cap_rank': [1],
        'trading_volume_24h': [25000000000],
        'price_change_24h': [2.5],
        'timestamp': [datetime.now()]
    })
    
    result = db.store_cryptocurrency(test_df, True)
    print(f"Inserted: {result}")
    
    # Verify
    db.verify_insert('cryptocurrency_data')
    db.get_table_stats()
    
    db.disconnect()