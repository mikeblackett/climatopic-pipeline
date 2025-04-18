import os

from dagster import (
    Definitions,
    FilesystemIOManager,
)
from upath import UPath

from climatopic.core.resources import HadukIOManager, XarrayZarrIOManager
from .climate import haduk, variables, indicators, climatology

base_dir: str = os.getenv('DAGSTER_LOCAL_ARTIFACT_STORAGE_DIR', 'data')

shared_resources = Definitions(
    resources={
        'io_manager': FilesystemIOManager(base_dir=base_dir),
        'xarray_zarr_io_manager': XarrayZarrIOManager(
            base_path=UPath(base_dir),
            processes=True,
            n_workers=1,
            threads_per_worker=8,
            memory_limit=0.85,
            persistence_mode='w',
        ),
        'haduk_io_manager': HadukIOManager(
            base_path=UPath(os.getenv('HADUK_ROOT', '')),
            version=os.getenv('HADUK_VERSION', ''),
            resolution=os.getenv('HADUK_RESOLUTION', ''),
            release=os.getenv('HADUK_RELEASE', ''),
        ),
    },
)

defs: Definitions = Definitions.merge(
    shared_resources,
    haduk.defs,
    variables.defs,
    indicators.defs,
    climatology.defs,
)
