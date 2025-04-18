from dagster import AssetChecksDefinition

from climatopic.core.checks import (
    build_xarray_chunk_check,
    build_xarray_cf_attributes_check,
)
from climatopic.core.constants import CHUNKS_PERCENTILE
from .assets import assets

checks: list[AssetChecksDefinition] = []
for asset in assets:
    checks.extend(
        [
            build_xarray_cf_attributes_check(asset.key),
            build_xarray_chunk_check(asset.key, chunks=CHUNKS_PERCENTILE),
        ]
    )
