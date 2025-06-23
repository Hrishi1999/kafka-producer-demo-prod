"""Main entry point for SQL Kafka Producer"""

import click
import yaml
import sys
from pathlib import Path

from .models import AppConfig
from .core import SQLKafkaProducer


@click.command()
@click.option(
    '--config', '-c',
    default='config.yaml',
    help='Configuration file path'
)
@click.option(
    '--query-id',
    help='Run specific query ID'
)
def main(config, query_id):
    """SQL to Kafka Producer"""
    
    # Load configuration
    try:
        config_path = Path(config)
        if not config_path.exists():
            click.echo(f"Configuration file not found: {config}", err=True)
            sys.exit(1)
        
        with open(config_path) as f:
            config_data = yaml.safe_load(f)
        
        app_config = AppConfig.from_yaml(config_data)
        
    except Exception as e:
        click.echo(f"Configuration error: {e}", err=True)
        sys.exit(1)
    
    # Initialize application
    try:
        app = SQLKafkaProducer(app_config)
        app.initialize()
        
        if query_id:
            # Run specific query
            query = next((q for q in app_config.queries if q.id == query_id), None)
            if not query:
                click.echo(f"Query not found: {query_id}", err=True)
                sys.exit(1)
            
            app.run_query(query)
        else:
            # Run application
            app.run()
            
    except KeyboardInterrupt:
        click.echo("\nShutdown requested...")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()