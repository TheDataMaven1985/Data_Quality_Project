# 📊 API Data Pipeline

> A robust, automated data pipeline that fetches data from multiple APIs, validates quality, stores in MySQL, and generates interactive dashboards.

[View Live Dashboard](https://thedatamaven1985.github.io/Data_Quality_Project/dashboard.html) | [Report Bug](https://github.com/TheDataMaven1985/Data_Quality_Project/issues) | [Report Features](https://github.com/TheDataMaven1985/Data_Quality_Project/issues)
---
Re
## 🌟 Features

- **🔄 Multi-API Integration** - Fetches real-time data from CoinGecko, DummyJSON, and OpenWeatherMap
- **✅ Data Validation** - Comprehensive quality checks (missing values, duplicates, type validation)
- **💾 MySQL Storage** - Efficient database storage with duplicate prevention
- **📊 Interactive Dashboard** - Real-time HTML dashboard with statistics and visualizations
- **🤖 Automated Execution** - Runs automatically every 6 hours via GitHub Actions
- **📝 Comprehensive Logging** - Track all operations, errors, and performance metrics
- **🧪 Tested** - Unit tests with pytest and coverage reporting

### Pipeline Execution
```
======================================================================
STARTING API DATA PIPELINE
======================================================================
📊 FETCHING CRYPTOCURRENCY DATA
✅ Fetched 20 cryptocurrency records
✅ Cryptocurrency data validation PASSED
✅ Stored 20 cryptocurrency records in database

📝 FETCHING POSTS DATA
✅ Fetched 50 posts records
✅ Posts data validation PASSED
✅ Stored 50 posts records in database

✅ PIPELINE COMPLETE - DATA SUCCESSFULLY STORED IN DATABASE
======================================================================
```

---

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- MySQL 8.0+
- Git

### Installation

```bash
# 1. Clone repository
git clone https://github.com/yourusername/data-pipeline.git
cd data-pipeline

# 2. Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Setup environment variables
cp .env.example .env
# Edit .env with your credentials

# 6. Run pipeline
python main.py

# 7. View dashboard
open dashboard.html  # or your browser
```

---

## 📁 Project Structure

```
data-pipeline/
├── src/                    # Source code
│   ├── api_fetcher.py     # API data fetching
│   ├── api_validator.py   # Data validation
│   ├── api_data_store.py  # Database operations
│   ├── api_dashboard.py   # Dashboard generation
│   └── quality_check.py   # Quality checks
├── tests/                  # Unit tests
│   ├── test_api_fetcher.py
│   ├── test_validator.py
│   └── test_database.py
├── scripts/                # Utility scripts
│   ├── run_pipeline.sh
│   ├── setup_cron.sh
│   └── export_data.py
├── .github/workflows/      # GitHub Actions
├── main.py                 # Main orchestrator
└── requirements.txt        # Dependencies
```

---

## 🔧 Configuration

### Environment Variables (`.env`)

```env
# Database
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=data_pipeline

# API Keys (optional)
WEATHER_API_KEY=your_openweathermap_key
```
---

## 📊 Data Sources

| API | Data Type | Update Frequency | API Key Required |
|-----|-----------|------------------|------------------|
| [CoinGecko](https://www.coingecko.com/en/api) | Cryptocurrency prices & market data | Every 6 hours | No |
| [DummyJSON](https://dummyjson.com/) | Sample posts and user data | Every 6 hours | No |
| [OpenWeatherMap](https://openweathermap.org/api) | Weather data | Every 6 hours | Yes (optional) |

---

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# View coverage report
open htmlcov/index.html
```

---

## 🤖 Automation

### Local (Cron Job)

```bash
# Setup cron job (runs every 30 minutes)
./scripts/setup_cron.sh

# Or manually add to crontab
crontab -e
# Add: */30 * * * * /path/to/data-pipeline/scripts/run_pipeline.sh
```

### Cloud (GitHub Actions)

The pipeline runs automatically on GitHub Actions:
- **Schedule**: Every 6 hours
- **Triggers**: Push to main/dev, Pull requests
- **Manual**: Via GitHub Actions UI
- **Note**: Scheduled runs occur at 01:00, 07:00, 13:00, and 19:00 WAT.

View workflow: [Actions](https://github.com/TheDataMaven1985/Data_Quality_Project/actions)

---

## 📈 Monitoring

### Logs

```bash
# View recent logs
tail -f logs/pipeline.log

# Check errors
grep ERROR logs/pipeline.log

# Monitor execution
tail -100 logs/pipeline.log
```

### Database

```bash
# Check record counts
mysql -u root -p data_quality_db -e "
  SELECT 
    'Cryptocurrencies' as Table_Name, 
    COUNT(*) as Records 
  FROM cryptocurrency_data
  UNION ALL
  SELECT 'Posts', COUNT(*) FROM posts_data;
"
```

### Dashboard

Open `dashboard.html` in your browser to view:
- Total records from each API
- Latest cryptocurrency prices
- Recent posts
- Data quality metrics
- Last update timestamp

---

## 🏗️ Architecture

```
┌─────────────┐
│   APIs      │  CoinGecko, DummyJSON, OpenWeatherMap
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Fetcher    │  Requests data with retry logic
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Validator  │  Quality checks & type validation
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   MySQL     │  Stores validated data
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Dashboard  │  Generates HTML visualization
└─────────────┘
```

---

## 🛠️ Development

### Setup Development Environment

```bash
# Install dev dependencies
pip install -r requirements-dev.txt

# Install pre-commit hooks
pre-commit install

# Run linting
flake8 src/
black src/

# Run type checking
mypy src/
```
### Adding New APIs

1. Add fetch method in `src/api_fetcher.py`
2. Add validation in `src/api_validator.py`
3. Add store method in `src/api_data_store.py`
4. Update `main.py` to call new methods
5. Add unit tests in `tests/`

---

## 🐛 Troubleshooting

### Database Connection Failed

```bash
# Check MySQL is running
sudo service mysql status
sudo service mysql start

# Test connection
mysql -u root -p -e "SELECT 1;"
```

### Module Not Found

```bash
# Make sure you're in project root
pwd

# Activate virtual environment
source .venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt
```

### Cron Job Not Running

```bash
# Check cron service
sudo service cron status

# View cron logs
grep CRON /var/log/syslog | tail -20

# Test script manually
./scripts/run_pipeline.sh
```

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'feat: Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Commit Convention

```bash
feat: Add new feature
fix: Fix bug
docs: Update documentation
test: Add tests
refactor: Refactor code
chore: Update dependencies
```

---

## 👤 Author

**Your Name**

- GitHub: [@TheDataMaven1985](https://github.com/TheDataMaven1985)
- LinkedIn: [Favour Kolawole](linkedin.com/in/favour-kolawole-b33a40287)
- Email: kolawolefavour20@gmail.com

---

## 🙏 Acknowledgments

- [CoinGecko](https://www.coingecko.com/en/api) for cryptocurrency data
- [DummyJSON](https://dummyjson.com/) for test data
- [OpenWeatherMap](https://openweathermap.org/api) for weather data
- GitHub Actions for free CI/CD

---

<div align="center">

**If you found this project helpful, please consider giving it a ⭐!**

Made with ❤️ and ☕

</div>
