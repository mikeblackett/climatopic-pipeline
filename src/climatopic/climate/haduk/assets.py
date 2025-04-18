from dagster import (
    StaticPartitionsDefinition,
)

from climatopic.core.asset_factory import AssetGroup, AssetTemplate

templates_by_output_name = {
    'tasmin': AssetTemplate(
        key='tasmin',
        description='Minimum air temperature',
    ),
    'tasmax': AssetTemplate(
        key='tasmax',
        description='Maximum air temperature',
    ),
    'tas': AssetTemplate(
        key='tas',
        description='Mean air temperature',
    ),
    'rainfall': AssetTemplate(
        key='rainfall',
        description='Total precipitation',
    ),
}

group = AssetGroup(
    name='haduk',
    description='HadUK-Grid climate variables',
    kinds={'netcdf', 'source'},
    key_prefix=['haduk'],
    partitions_def=StaticPartitionsDefinition(['day', 'mon']),
    io_manager_key='haduk_io_manager',
    templates=templates_by_output_name.values(),
)

assets = group.to_assets_def()
