#!/bin/bash

# Install the package in development mode
pip install -e .

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 10

# Execute scraping commands
echo "Starting model scraping..."
python ./src/main.py scrape -t model -l 50 --tags text-generation,text-classification,sentence-similarity,test-generation,text2text-generation

echo "Starting dataset scraping..."
python ./src/main.py scrape -t dataset -l 25 --tags task_categories:text-generation,task_categories:text-classification,task_categories:text2text-generation,task_categories:sentence-similarity,task_categories:token-classification

echo "Starting organization scraping..."
python ./src/main.py scrape -s org

echo "Starting collection scraping..."
python ./src/main.py scrape -s collection

echo "Building graph..."
python ./src/main.py build-graph

# Keep container running
tail -f /dev/null 