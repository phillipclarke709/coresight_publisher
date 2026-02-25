import sys
from pathlib import Path

import click
from loguru import logger

from coresight_processingchain.sentinel_pairs.coresight_publisher.publisher import publish_geotiff, publish_geojson


def configure_logging(verbose: bool):
    logger.remove()
    logger.add(
        sys.stderr if verbose else sys.stdout, 
        level="DEBUG" if verbose else "INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
    )


@click.group()
@click.option('-v', '--verbose', is_flag=True, help='Enable verbose mode.')
@click.pass_context
def cli(ctx, verbose):
    ctx.ensure_object(dict)
    ctx.obj['VERBOSE'] = verbose
    configure_logging(verbose)


PUBLISHERS = {
    "coherence": publish_geotiff,
    "spo": publish_geotiff,
    "floe_edge_polygons": publish_geojson,
    "ice_drift": publish_geojson
}

@cli.command()
@click.pass_context
@click.option('-p', '--path', type=str, required=True, help='Path of file to upload.')
@click.option('-t', '--product_type', 
    type=click.Choice(
        ['coherence', 'spo', 'floe_edge_polygons', 'ice_drift'], 
        case_sensitive=False
    ), 
    required=True, 
    help='Type of ice product (coherence, spo, floe_edge_polygons, ice_drift).'
)
def publish(ctx, path: str | Path, product_type: str):
    publisher = PUBLISHERS.get(product_type)
    publisher(path, product_type)
    

if __name__ == '__main__':
    cli(obj={})
    
