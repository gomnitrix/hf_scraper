import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.live import Live
from typing import List, Optional
import os
import asyncio
from scraper.model_scraper import ModelScraper
from scraper.dataset_scraper import DatasetScraper
from scraper.org_scraper import OrganizationScraper
from scraper.collection_scraper import CollectionScraper
from graph.migrator import GraphMigrator

console = Console()

@click.group()
def cli():
    """Hugging Face Resource Scraper and Graph Builder."""
    pass

@cli.command()
@click.option('--type', '-t', 
              type=click.Choice(['model', 'dataset']),
              default='model',
              help='Type of resources to scrape')
@click.option('--subtype', '-s',
              type=click.Choice(['org', 'collection']),
              help='Specify resource subtypes to scrape (requires main type to be scraped first)')
@click.option('--limit', '-l',
              type=int,
              help='Maximum number of items to scrape')
@click.option('--tags', '-g',
              help='Comma-separated list of tags to filter by')
@click.option('--rate-limit', '-r',
              type=int,
              default=10,
              help='Rate limit (requests per second)')
@click.option('--batch-size', '-b',
              type=int,
              default=64,
              help='Batch size for processing items')
@click.option('--min-upvotes', 
              type=int, 
              default=100, 
              help='Minimum number of upvotes for collections')
@click.option('--resume', 
              is_flag=True,
              help='Resume failed scraping tasks')
def scrape(type: str, 
         subtype: str,
         limit: Optional[int], 
         tags: Optional[str], 
         rate_limit: int,
         batch_size: int,
         min_upvotes: int,
         resume: bool):
    """Scrape metadata from Hugging Face models and datasets."""
    # Get environment variables
    mongo_uri = os.getenv('MONGODB_URI')
    redis_uri = os.getenv('REDIS_URI')
    
    tag_list = tags.split(',') if tags else None
    
    async def run_scrapers():
        scraper = None
        if type == 'model':
            scraper = ModelScraper(
                mongo_uri=mongo_uri,
                redis_uri=redis_uri,
                limit=limit,
                tags=tag_list,
                rate_limit=rate_limit,
                batch_size=batch_size
            )
            
        if type == 'dataset':
            scraper = DatasetScraper(
                mongo_uri=mongo_uri,
                redis_uri=redis_uri,
                limit=limit,
                tags=tag_list,
                rate_limit=rate_limit,
                batch_size=batch_size
            )
        
        if subtype == 'org':
            scraper = OrganizationScraper( 
                mongo_uri=mongo_uri,
                redis_uri=redis_uri,
                rate_limit=rate_limit,
                batch_size=batch_size
            )
            
        if subtype == 'collection':
            scraper = CollectionScraper(
                mongo_uri=mongo_uri,
                redis_uri=redis_uri,
                rate_limit=rate_limit,
                batch_size=batch_size,
                min_upvotes=min_upvotes,
            )
            
        # Run scrapers
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[cyan]{task.description}"),
        )

        with Live(progress, refresh_per_second=10, console=console):
            if resume:
                extended_task = progress.add_task(f"Scraping {scraper.item_type} extended metadata...", total=None)
                await scraper.process_extended_tasks(progress, extended_task)
            else:
                basic_task = progress.add_task(f"Scraping {scraper.item_type} base metadata...", total=None)
                extended_task = progress.add_task(f"Scraping {scraper.item_type} extended metadata...", total=None)
                await asyncio.gather(
                    scraper.scrape(progress, basic_task),
                    scraper.process_extended_tasks(progress, extended_task)
                )
        
        # Display results summary
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Type")
        table.add_column("Total")
        table.add_column("Basic")
        table.add_column("Extended")
        
        collection = scraper.item_type
        stats = await scraper.mongo_client.get_stats(collection)
        
        table.add_row(
            collection.capitalize(),
            str(stats["total"]),
            str(stats["basic"]),
            str(stats["extended"])
        )
        console.print(table)
    asyncio.run(run_scrapers())

@cli.command()
def build_graph():
    """Build graph from MongoDB to Dgraph."""
    mongo_uri = os.getenv('MONGODB_URI')
    dgraph_uri = os.getenv('DGRAPH_URI')
    
    async def run_migration():
        migrator = GraphMigrator(mongo_uri, dgraph_uri)
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[cyan]{task.description}"),
        )
        
        with Live(progress, refresh_per_second=10, console=console):
            migration_task = progress.add_task("Migrating data to Dgraph...", total=None)
            await migrator.migrate_all()
            progress.update(migration_task, completed=True)
        
        console.print("[green]Migration completed successfully![/green]")
    
    asyncio.run(run_migration())

if __name__ == '__main__':
    cli() 