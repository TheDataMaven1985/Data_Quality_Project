import mysql.connector
from datetime import datetime, timedelta
import json
from pathlib import Path
import logging
import os
from dotenv import load_dotenv

load_dotenv()

# SETUP LOGGING
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class APIDashboard:
    """
    Generate HTML dashboard from API data in MySQL
    
    Shows:
    - Total records from each API
    - Data quality metrics
    - Last fetch time
    - Recent data samples
    """

    def __init__(self, output_file: str = 'dashboard.html'):
        """Initialize Dashboard Generator"""
        self.output_file = output_file
        self.logger = logging.getLogger('Dashboard')
        self.connection = None

    def connect(self) -> bool:
        """Connect to MySQL Database"""
        try:
            self.connection = mysql.connector.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD', ''),
                database=os.getenv('DB_NAME')
            )
            return self.connection.is_connected()
        except mysql.connector.Error as err:
            self.logger.error(f"Database connection error: {err}")
            return False
    
    def get_crypto_stats(self) -> dict:
        """Fetch cryptocurrency stats from database"""
        try:
            cursor = self.connection.cursor(dictionary=True)
            
            # Overall stats
            cursor.execute("""
            SELECT 
              COUNT(*) as total_cryptos,
              AVG(current_price) as avg_price,
              MAX(current_price) as max_price,
              MIN(current_price) as min_price,
              SUM(market_cap) as total_market_cap,
              MAX(timestamp) as last_updated
            FROM cryptocurrency_data
            WHERE validation_passed = TRUE
            """)
            
            stats = cursor.fetchone()

            # Top cryptos by market cap
            cursor.execute("""
            SELECT symbol, name, current_price, market_cap_rank, price_change_24h
            FROM cryptocurrency_data
            WHERE validation_passed = TRUE
            ORDER BY market_cap_rank ASC
            LIMIT 5
            """)
            
            top_cryptos = cursor.fetchall()
            
            cursor.close()
            
            return {
                'total': stats['total_cryptos'] or 0,
                'avg_price': round(stats['avg_price'] or 0, 2),
                'max_price': round(stats['max_price'] or 0, 2),
                'total_market_cap': int(stats['total_market_cap'] or 0),
                'last_updated': stats['last_updated'],
                'top_cryptos': top_cryptos or []
            }
        
        except Exception as e:
            self.logger.error(f"Error getting crypto stats: {e}")
            return {}

    def get_posts_stats(self) -> dict:
        """Get statistics about posts data"""
        try:
            cursor = self.connection.cursor(dictionary=True)
            
            # Overall stats
            cursor.execute("""
            SELECT 
              COUNT(*) as total_posts,
              AVG(word_count) as avg_words,
              MAX(word_count) as max_words,
              MIN(word_count) as min_words,
              COUNT(DISTINCT user_id) as unique_users,
              MAX(timestamp) as last_updated
            FROM posts_data
            WHERE validation_passed = TRUE
            """)
            
            stats = cursor.fetchone()
            
            # Recent posts
            cursor.execute("""
            SELECT post_id, user_id, title, word_count, timestamp
            FROM posts_data
            WHERE validation_passed = TRUE
            ORDER BY timestamp DESC
            LIMIT 5
            """)
            
            recent_posts = cursor.fetchall()
            
            cursor.close()
            
            return {
                'total': stats['total_posts'] or 0,
                'avg_words': int(stats['avg_words'] or 0),
                'max_words': stats['max_words'] or 0,
                'unique_users': stats['unique_users'] or 0,
                'last_updated': stats['last_updated'],
                'recent_posts': recent_posts or []
            }
        
        except Exception as e:
            self.logger.error(f"Error getting posts stats: {e}")
            return {}

    def generate_html(self):
        """Generate HTML dashboard from data"""
        
        # Get all stats
        cryptos = self.get_crypto_stats()
        posts = self.get_posts_stats()
        
        # Format timestamps
        cryptos_time = cryptos.get('last_updated', 'Never')
        posts_time = posts.get('last_updated', 'Never')
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>API Data Pipeline Dashboard</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: 'Inter', 'Segoe UI', Tahoma, sans-serif;
            background: linear-gradient(135deg, #eef2ff 0%, #f8fafc 100%);
            color: #1f2937;
            min-height: 100vh;
            padding: 24px;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}

        header {{
            background: white;
            padding: 32px;
            border-radius: 16px;
            margin-bottom: 32px;
            box-shadow: 0 8px 24px rgba(0,0,0,0.08);
            border-left: 6px solid #6366f1;
        }}

        h1 {{
            color: #4338ca;
            margin-bottom: 8px;
            font-size: 2rem;
            font-weight: 700;
        }}

        .timestamp {{
            color: #6b7280;
            font-size: 0.9rem;
        }}

        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
            gap: 24px;
            margin-bottom: 32px;
        }}

        .card {{
            background: white;
            padding: 28px;
            border-radius: 16px;
            box-shadow: 0 8px 24px rgba(0,0,0,0.08);
            transition: transform 0.25s ease, box-shadow 0.25s ease;
            border-top: 4px solid #6366f1;
        }}

        .card:hover {{
            transform: translateY(-6px);
            box-shadow: 0 12px 32px rgba(0,0,0,0.12);
        }}

        .card h2 {{
            color: #4338ca;
            margin-bottom: 18px;
            font-size: 1.4rem;
            font-weight: 600;
        }}

        .stat {{
            margin: 12px 0;
            padding: 14px 16px;
            background: #f9fafb;
            border-radius: 10px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}

        .stat-label {{
            color: #6b7280;
            font-size: 0.85rem;
            font-weight: 500;
        }}

        .stat-value {{
            color: #111827;
            font-size: 1.4rem;
            font-weight: 700;
        }}

        h3 {{
            margin-top: 24px;
            margin-bottom: 12px;
            color: #374151;
            font-size: 1.05rem;
            font-weight: 600;
        }}

        .table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 12px;
            font-size: 0.9rem;
        }}

        .table th {{
            background: #eef2ff;
            padding: 12px;
            text-align: left;
            font-weight: 600;
            color: #4338ca;
            border-bottom: 2px solid #c7d2fe;
        }}

        .table td {{
            padding: 12px;
            border-bottom: 1px solid #e5e7eb;
            color: #374151;
        }}

        .table tr:hover {{
            background: #f9fafb;
        }}

        .footer {{
            text-align: center;
            margin-top: 40px;
            color: #6b7280;
            font-size: 0.9rem;
        }}

        .api-indicator {{
            display: inline-block;
            width: 10px;
            height: 10px;
            border-radius: 50%;
            background: #22c55e;
            margin-right: 8px;
            box-shadow: 0 0 0 4px rgba(34,197,94,0.15);
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>📊 API Data Pipeline Dashboard</h1>
            <div class="timestamp">Last Updated: {current_time}</div>
        </header>
        
        <div class="grid">
            <!-- CRYPTO CARD -->
            <div class="card">
                <h2>💰 Cryptocurrency Data</h2>
                
                <div class="stat">
                    <div class="stat-label">Total Cryptocurrencies</div>
                    <div class="stat-value">{cryptos.get('total', 0)}</div>
                </div>
                
                <div class="stat">
                    <div class="stat-label">Average Price</div>
                    <div class="stat-value">${cryptos.get('avg_price', 0):,.2f}</div>
                </div>
                
                <div class="stat">
                    <div class="stat-label">Highest Price</div>
                    <div class="stat-value">${cryptos.get('max_price', 0):,.2f}</div>
                </div>
                
                <div class="stat">
                    <div class="stat-label">Total Market Cap</div>
                    <div class="stat-value">${cryptos.get('total_market_cap', 0):,}</div>
                </div>
                
                <div class="stat">
                    <div class="stat-label">Last Updated</div>
                    <div class="stat-value" style="font-size: 0.9em;">{cryptos_time}</div>
                </div>
                
                <h3>Top 5 by Market Cap</h3>
                <table class="table">
                    <thead>
                        <tr>
                            <th>Symbol</th>
                            <th>Price</th>
                            <th>24h Change</th>
                        </tr>
                    </thead>
                    <tbody>
"""
        
        # Add top cryptos
        for crypto in cryptos.get('top_cryptos', []):
            change = crypto.get('price_change_24h', 0)
            change_color = '#10b981' if change > 0 else '#ef4444'
            html_content += f"""                        <tr>
                            <td><strong>{crypto['symbol']}</strong></td>
                            <td>${crypto['current_price']:,.2f}</td>
                            <td style="color: {change_color};">{change:+.2f}%</td>
                        </tr>
"""
        
        html_content += f"""                    </tbody>
                </table>
            </div>
            
            <!-- POSTS CARD -->
            <div class="card">
                <h2>📝 Posts Data</h2>
                
                <div class="stat">
                    <div class="stat-label">Total Posts</div>
                    <div class="stat-value">{posts.get('total', 0)}</div>
                </div>
                
                <div class="stat">
                    <div class="stat-label">Unique Users</div>
                    <div class="stat-value">{posts.get('unique_users', 0)}</div>
                </div>
                
                <div class="stat">
                    <div class="stat-label">Average Words</div>
                    <div class="stat-value">{posts.get('avg_words', 0)}</div>
                </div>
                
                <div class="stat">
                    <div class="stat-label">Max Words in Post</div>
                    <div class="stat-value">{posts.get('max_words', 0)}</div>
                </div>
                
                <div class="stat">
                    <div class="stat-label">Last Updated</div>
                    <div class="stat-value" style="font-size: 0.9em;">{posts_time}</div>
                </div>
                
                <h3>Recent Posts</h3>
                <table class="table">
                    <thead>
                        <tr>
                            <th>Post ID</th>
                            <th>User</th>
                            <th>Words</th>
                        </tr>
                    </thead>
                    <tbody>
"""
        
        # Add recent posts
        for post in posts.get('recent_posts', []):
            html_content += f"""                        <tr>
                            <td>{post['post_id']}</td>
                            <td>User {post['user_id']}</td>
                            <td>{post['word_count']}</td>
                        </tr>
"""
        
        html_content += f"""                    </tbody>
                </table>
            </div>
        </div>
        
        <!-- FOOTER -->
        <div class="footer">
            <span class="api-indicator"></span>
            <p>All APIs operational • Last refresh: {current_time}</p>
        </div>
    </div>
</body>
</html>
"""
        
        # Write to file
        Path(self.output_file).write_text(html_content)
        self.logger.info(f"Dashboard saved to {self.output_file}")
        return self.output_file
    
    def disconnect(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.logger.info("Database connection closed.")
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()


# Main execution
if __name__ == "__main__":
    print("="*70)
    print("GENERATING DASHBOARD")
    print("="*70)
    
    with APIDashboard(output_file='dashboard.html') as dashboard:
        if dashboard.connection and dashboard.connection.is_connected():
            print("Connected to database")
            
            output_file = dashboard.generate_html()
            
            print(f"\nDashboard generated successfully!")
            print(f"File: {output_file}")
            print(f"Open in browser: file://{os.path.abspath(output_file)}")
        else:
            print("Failed to connect to database")
            print("Check your .env file and ensure MySQL is running")
    
    print("="*70)