from dagster import Definitions, load_assets_from_modules

from . import assets
from .assets import templates_by_output_name

__all__ = ['defs', 'templates_by_output_name']

defs = Definitions(
    assets=load_assets_from_modules(
        modules=[assets],
        group_name='indicators',
    ),
    # asset_checks=load_asset_checks_from_modules(
    #     [checks], asset_key_prefix=KEY_PREFIX
    # ),
)
