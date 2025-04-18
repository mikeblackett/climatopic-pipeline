import xarray as xr
from climatopic_xarray.frequencies import Frequency
from dagster import (
    AssetExecutionContext,
    Output,
)

from climatopic.climate.indicators import (
    templates_by_output_name as input_templates,
)
from climatopic.core.asset_factory import AssetTemplate, asset_factory
from climatopic.core.constants import CHUNKS_TIME
from climatopic.core.metadata.metadata import generate_dataset_asset_metadata
from climatopic.core.utilities import (
    dataset_to_data_array,
    get_partition_key,
)


templates_by_output_name = {
    name: AssetTemplate(
        key=name,
        description=template.description,
        ins={
            name: template.to_in(
                key_prefix='indicators',
            )
        },
        partitions_def=template.partitions_def,
        tags=template.tags,
    )
    for name, template in input_templates.items()
}


@asset_factory(
    name='climatologies',
    description='Climate indicator climatologies',
    kinds={'Xarray', 'Zarr'},
    key_prefix=['climatologies'],
    io_manager_key='xarray_zarr_io_manager',
    # dagster_type=IndicatorDagsterType,
    # metadata=IndicatorDagsterType.metadata,
    templates=templates_by_output_name.values(),
)
def assets(
    context: AssetExecutionContext, **ins_kwargs: xr.Dataset
) -> Output[xr.Dataset]:
    ds: xr.Dataset = next(iter(ins_kwargs.values()))
    da: xr.DataArray = dataset_to_data_array(ds)
    frequency = Frequency(
        get_partition_key(context=context, dimension='frequency').upper()
    )
    ds: xr.Dataset = (
        da.climatology(frequency=frequency.name)
        .mean('time')
        .to_dataset()
        .bounds.add()
        .chunk(CHUNKS_TIME)
    )
    return Output(
        value=ds,
        metadata=generate_dataset_asset_metadata(ds),
    )


# def build_partition_mapping(
#     partitions_def: PartitionsDefinition,
# ) -> PartitionsDefinition | None:
#     if isinstance(partitions_def, MultiPartitionsDefinition):
#         if partitions_def.partition_dimension_names != [
#             'frequency',
#             'baseline',
#         ]:
#             raise ValueError(
#                 'expected partition_dimension_names to be ["frequency", "baseline"]'
#             )
#         return MultiPartitionsDefinition(
#             {
#                 'frequency': partitions_def.get_partitions_def_for_dimension(
#                     'frequency'
#                 ),
#                 'baseline': partitions_def.partition_defs['baseline'],
#             }
#         )
#     return None
