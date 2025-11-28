#!/bin/bash

# Startup script for Airflow pipeline

echo "=========================================="
echo "Airflow Data Pipeline Setup"
echo "=========================================="
echo ""

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env file..."
    echo "AIRFLOW_UID=$(id -u)" > .env
    echo "✓ .env file created"
else
    echo "✓ .env file already exists"
fi

# Create data directories
echo ""
echo "Creating data directories..."
mkdir -p logs data/hourly_analysis data/error_analysis
echo "✓ Directories created"

# Start Docker Compose
echo ""
echo "Starting Docker containers..."
echo "This may take a few minutes on first run..."
docker-compose up -d

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Access Airflow Web UI at: http://localhost:8080"
echo "Username: airflow"
echo "Password: airflow"
echo ""
echo "To check container status: docker-compose ps"
echo "To view logs: docker-compose logs -f"
echo "To stop: docker-compose down"
echo ""
echo "Wait 1-2 minutes for Airflow to initialize,"
echo "then activate the DAGs in the web interface."
echo ""
