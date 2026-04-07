import sys
import time
from pathlib import Path

import click
from loguru import logger

from publisher import (
    publish_geotiff,
    publish_geojson,
    remove_product,
    remove_batch,
)


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


@cli.command()
@click.pass_context
@click.option('-c', '--collection_id', type=str, required=True, help='STAC collection id.')
@click.option('-a', '--asset_name', type=str, required=True, help='Asset filename in the GCP bucket.')
@click.option('-i', '--item_id', type=str, default=None, help='Explicit STAC item id. Defaults to asset name without extension.')
@click.option('--no-manual-confirmation', is_flag=True, help='Skip manual checkpoint after STAC deletion.')
def remove(ctx, collection_id: str, asset_name: str, item_id: str | None, no_manual_confirmation: bool):
    """Safely remove a product by deleting STAC item first, then bucket asset."""
    # The CLI only collects inputs; the actual safety checks live in publisher.remove_product.
    success = remove_product(
        collection_id=collection_id,
        asset_name=asset_name,
        item_id=item_id,
        verbose=ctx.obj['VERBOSE'],
        require_manual_confirmation=not no_manual_confirmation,
    )
    if not success:
        raise click.ClickException("Product removal did not complete.")


@cli.command(name="remove-batch")
@click.pass_context
@click.option('-c', '--collection_id', type=str, required=True, help='STAC collection id.')
@click.option('-p', '--asset-pattern', '--asset_pattern', type=str, required=True, help='Regex pattern to match asset filenames.')
def remove_batch_command(ctx, collection_id: str, asset_pattern: str):
    """Safely remove multiple products whose asset names match a regex."""
    deleted_products = remove_batch(
        collection_id=collection_id,
        asset_pattern=asset_pattern,
        verbose=ctx.obj['VERBOSE'],
    )
    if not deleted_products:
        click.echo("No products were deleted.")
    time.sleep(1)
    click.echo("Clearing terminal")
    time.sleep(0.5)
    click.clear()


if __name__ == '__main__':
    cli(obj={})
    
