from dagster import (
    Definitions,
    load_assets_from_modules,
    load_asset_checks_from_modules,
)

from . import percentile, primary
from .percentile import (
    templates_by_output_name as percentile_templates_by_output_name,
)
from .primary import (
    checks,
    templates_by_output_name as primary_templates_by_output_name,
)

templates_by_output_name = (
    primary_templates_by_output_name | percentile_templates_by_output_name
)

__all__ = [
    'templates_by_output_name',
    'defs',
    'checks',
]

defs = Definitions(
    assets=load_assets_from_modules(
        modules=[primary, percentile],
        group_name='variables',
    ),
    asset_checks=load_asset_checks_from_modules([checks]),
)
