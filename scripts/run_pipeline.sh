#!/bin/bash

# Absolute paths (cron requires this)
PROJECT_DIR="/mnt/c/Users/MY LAPTOP/data-pipeline"
VENV_DIR="$PROJECT_DIR/.venv"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/pipeline.log"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Log start time
echo "======================================" >> "$LOG_FILE"
echo "Pipeline started: $(date)" >> "$LOG_FILE"
echo "======================================" >> "$LOG_FILE"

# Navigate to project directory
cd "$PROJECT_DIR" || exit 1

# Activate virtual environment
source "$VENV_DIR/bin/activate"

# Run pipeline and log output
python main.py >> "$LOG_FILE" 2>&1

# Capture exit code
EXIT_CODE=$?

# Log completion
if [ $EXIT_CODE -eq 0 ]; then
    echo " Pipeline completed: $(date)" >> "$LOG_FILE"
else
    echo " Pipeline failed with code $EXIT_CODE: $(date)" >> "$LOG_FILE"
fi

# Deactivate venv
deactivate

echo "" >> "$LOG_FILE"

exit $EXIT_CODE