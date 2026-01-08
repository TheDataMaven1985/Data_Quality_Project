import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# PATHS
PROJECT_ROOT = Path(__file__).parent.parent
LOG_DIR = PROJECT_ROOT / os.getenv("LOG_DIR", "logs")
DASHBOARD_DIR = PROJECT_ROOT / os.getenv("DASHBOARD_DIR", "dashboards")
DATA_DIR = PROJECT_ROOT / os.getenv("DATA_DIR", "data")

# CREATE DIRECTORIES IF THEY DON'T EXIST
LOG_DIR.mkdir(parents=True, exist_ok=True)
DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

# DATABASE CONFIGURATION
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "user"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "database": os.getenv("DB_NAME", "data_quality")
}

# LOGGING CONFIGURATION
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# DASHBOARD CONFIGURATION
ARCHIVE_DASHBOARD = os.getenv("ARCHIVE_DASHBOARD", 'True').lower() == 'true'

# API KEYS (only if using paid/key-required APIs)
RUN_WEATHER = os.getenv("RUN_WEATHER", 'True').lower() == 'true'