from setuptools import setup, find_packages

setup(
    name="huggingface_scraper",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "huggingface_hub",
        "click",
        "rich",
        "python-dotenv",
        "aiohttp",
        "tenacity",
        "pymongo",
        "redis",
        "aioredis",
        "motor",
        "pydgraph"
    ],
    entry_points={
        'console_scripts': [
            'hf-scraper=src.main:main',
        ],
    },
)
