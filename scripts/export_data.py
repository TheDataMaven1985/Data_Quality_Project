cat > scripts/export_data.py << 'EOF'
"""
Export data from MySQL to CSV files
"""

import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api_data_store import APIDataStore
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataExporter:
    """Export database data to CSV files"""
    
    def __init__(self, output_dir: str = 'data'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.store = APIDataStore()
        self.timestamp = datetime.now().strftime('%Y%m%d')
    
    def export_cryptocurrency_data(self):
        """Export cryptocurrency data to CSV"""
        try:
            if not self.store.connection:
                self.store.connect()
            
            query = """
                SELECT 
                    symbol, name, current_price, market_cap, 
                    market_cap_rank, trading_volume_24h, 
                    price_change_24h, timestamp
                FROM cryptocurrency_data
                WHERE validation_passed = TRUE
                ORDER BY market_cap_rank ASC
            """
            
            cursor = self.store.connection.cursor(dictionary=True)
            cursor.execute(query)
            data = cursor.fetchall()
            cursor.close()
            
            if data:
                df = pd.DataFrame(data)
                filename = self.output_dir / f'cryptocurrencies_{self.timestamp}.csv'
                df.to_csv(filename, index=False)
                logger.info(f"Exported {len(df)} records to {filename}")
                return filename
            return None
        except Exception as e:
            logger.error(f"Error: {e}")
            return None
    
    def export_posts_data(self):
        """Export posts data to CSV"""
        try:
            if not self.store.connection:
                self.store.connect()
            
            query = """
                SELECT post_id, user_id, title, body, word_count, timestamp
                FROM posts_data
                WHERE validation_passed = TRUE
                ORDER BY timestamp DESC
            """
            
            cursor = self.store.connection.cursor(dictionary=True)
            cursor.execute(query)
            data = cursor.fetchall()
            cursor.close()
            
            if data:
                df = pd.DataFrame(data)
                filename = self.output_dir / f'posts_{self.timestamp}.csv'
                df.to_csv(filename, index=False)
                logger.info(f"Exported {len(df)} records to {filename}")
                return filename
            return None
        except Exception as e:
            logger.error(f"Error: {e}")
            return None
    
    def export_all(self):
        """Export all data"""
        logger.info("="*70)
        logger.info("EXPORTING DATA TO CSV")
        logger.info("="*70)
        
        files = []
        
        crypto_file = self.export_cryptocurrency_data()
        if crypto_file:
            files.append(crypto_file)
        
        posts_file = self.export_posts_data()
        if posts_file:
            files.append(posts_file)
        
        logger.info(f"Export complete! {len(files)} files created")
        return files


if __name__ == "__main__":
    exporter = DataExporter()
    exporter.export_all()
    exporter.store.disconnect()
EOF

chmod +x scripts/export_data.py
echo "Created scripts/export_data.py"