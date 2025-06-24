"""Main entry point for HTTP Kafka Consumer"""

import click
import yaml
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

from .models.config import ConsumerConfig
from .core.app import HTTPKafkaConsumer
from .core.retry_consumer import HTTPRetryConsumer
from .utils.monitoring import setup_logging


@click.command()
@click.option(
    '--config', '-c',
    default='consumer_config.yaml',
    help='Configuration file path'
)
@click.option(
    '--mode', '-m',
    type=click.Choice(['main', 'retry']),
    help='Consumer mode: main or retry (overrides config)'
)
@click.option(
    '--env-file',
    default='.env',
    help='Environment file path'
)
@click.option(
    '--log-level',
    type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
    help='Log level (overrides config)'
)
def main(config, mode, env_file, log_level):
    """HTTP Kafka Consumer
    
    Consumes messages from Kafka topics and sends them to HTTP endpoints.
    Supports retry processing and comprehensive error handling.
    """
    
    # Load environment variables
    env_path = Path(env_file)
    if env_path.exists():
        load_dotenv(env_path)
        click.echo(f"Loaded environment from: {env_path}")
    elif env_file != '.env':  # Only warn if explicitly specified
        click.echo(f"Warning: Environment file not found: {env_path}", err=True)
    
    # Load configuration
    try:
        config_path = Path(config)
        if not config_path.exists():
            click.echo(f"Configuration file not found: {config}", err=True)
            sys.exit(1)
        
        click.echo(f"Loading configuration from: {config_path}")
        with open(config_path) as f:
            config_data = yaml.safe_load(f)
        
        app_config = ConsumerConfig(**config_data)
        
    except Exception as e:
        click.echo(f"Configuration error: {e}", err=True)
        sys.exit(1)
    
    # Override mode from command line if provided
    if mode:
        app_config.consumer_mode.mode = mode
        click.echo(f"Mode overridden to: {mode}")
    
    # Override log level if provided
    if log_level:
        if hasattr(app_config.monitoring, 'log_level'):
            app_config.monitoring.log_level = log_level
        click.echo(f"Log level overridden to: {log_level}")
    
    # Setup logging
    try:
        if hasattr(app_config.monitoring, 'log_level'):
            setup_logging(
                log_level=getattr(app_config.monitoring, 'log_level', 'INFO'),
                log_format=getattr(app_config.monitoring, 'log_format', 'json'),
                log_file=getattr(app_config.monitoring, 'log_file', None)
            )
        else:
            # Fallback for simple consumer config
            import logging
            logging.basicConfig(
                level=getattr(logging, log_level or 'INFO'),
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
    except Exception as e:
        click.echo(f"Logging setup error: {e}", err=True)
        # Continue with basic logging
        import logging
        logging.basicConfig(level=logging.INFO)
    
    # Display startup information
    click.echo("=" * 60)
    click.echo("HTTP Kafka Consumer Starting")
    click.echo("=" * 60)
    click.echo(f"Mode: {app_config.consumer_mode.mode.upper()}")
    click.echo(f"Topics: {', '.join(app_config.kafka.topics)}")
    click.echo(f"Group ID: {app_config.kafka.group_id}")
    click.echo(f"HTTP Endpoint: {app_config.http.endpoint_url}")
    click.echo(f"Monitoring Port: {app_config.monitoring.port}")
    
    if app_config.consumer_mode.mode == "retry":
        retry_topics = [f"{topic}{app_config.retry.topic_suffix}" for topic in app_config.kafka.topics]
        click.echo(f"Retry Topics: {', '.join(retry_topics)}")
        click.echo(f"Retry Group ID: {app_config.kafka.group_id}-retry")
    
    click.echo("=" * 60)
    
    # Initialize and run consumer
    try:
        if app_config.consumer_mode.mode == "retry":
            consumer = HTTPRetryConsumer(app_config)
            click.echo("🔄 Starting RETRY consumer...")
        else:
            consumer = HTTPKafkaConsumer(app_config)
            click.echo("▶️  Starting MAIN consumer...")
        
        consumer.run()
        
    except KeyboardInterrupt:
        click.echo("\n🛑 Shutdown requested by user...")
    except Exception as e:
        click.echo(f"❌ Consumer error: {e}", err=True)
        sys.exit(1)
    finally:
        click.echo("✅ Consumer stopped")


if __name__ == "__main__":
    main()