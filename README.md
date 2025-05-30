# Hugging Face Resource Scraper

A tool for scraping and building graph from Hugging Face resources.

## Features

- Scrape metadata from Hugging Face models, datasets, organizations and collections
- Build graph using DGraph
- Also Store data in MongoDB
- Rate limiting and task management with Redis
- Docker support for easy deployment

## Quick Start with Docker

1. Clone the repository
2. Start the services:
```bash
cd docker
docker-compose up -d
```

This will:
- Start MongoDB, Redis, and DGraph services
- Run the scraper to collect data from Hugging Face
- Build the graph

## Manual Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
pip install -e .
```

2. Start required services (MongoDB, Redis, DGraph)

3. Run the scraper:
```bash
# Scrape models
python src/main.py scrape -t model -l 50 --tags text-generation,text-classification,sentence-similarity,test-generation,text2text-generation

# Scrape datasets
python src/main.py scrape -t dataset -l 25 --tags task_categories:text-generation,task_categories:text-classification,task_categories:text2text-generation,task_categories:sentence-similarity,task_categories:token-classification

# Scrape organizations
python src/main.py scrape -s org

# Scrape collections
python src/main.py scrape -s collection

# Build graph
python src/main.py build-graph
```

## Project Structure

```
src/
├── graph/              # Graph building
│   ├── migrator.py     # Main logic for graph building
│   └── schema.rdf      # DGraph schema
├── scraper/            # Scraping logic
│   ├── base_scraper.py # Base scraper class
│   ├── model_scraper.py
│   ├── dataset_scraper.py
│   ├── org_scraper.py
│   └── collection_scraper.py
├── utils/              # Utility functions
│   ├── dgraph_client.py
│   ├── mongodb.py
│   └── redis_client.py
└── main.py            # Entry point
```

## Dependencies

- huggingface-hub
- pydgraph
- pymongo
- redis
- aiohttp
- python-dotenv 