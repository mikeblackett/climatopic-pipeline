import xarray as xr
from dagster import (
    AssetExecutionContext,
    Output,
    SpecificPartitionsPartitionMapping,
)

from climatopic.climate.haduk import (
    templates_by_output_name as input_templates,
)
from climatopic.core.asset_factory import AssetTemplate, asset_factory
from climatopic.core.constants import CHUNKS_TIME
from climatopic.core.dagster_types import ParameterDagsterType
from climatopic.core.metadata import generate_dataset_asset_metadata
from climatopic.core.utilities import (
    datasets_to_data_arrays,
    get_input_names,
    keywords_to_tags,
)
from climatopic.core.variables import variables

templates_by_output_name = {
    name: AssetTemplate(
        key=name,
        description=variable.title,
        ins={
            input_name: input_templates[input_name].to_in(
                key_prefix='haduk',
                partition_mapping=SpecificPartitionsPartitionMapping(['day']),
            )
            for input_name in get_input_names(variable)
        },
        tags=keywords_to_tags(variable.keywords),
        metadata={
            key: str(value)
            for key, value in variable.injected_parameters.items()
        },
    )
    for name, variable in variables.filter_variables(
        names=['tas', 'tasmax', 'tasmin', 'pr']
    )
}


@asset_factory(
    name='variables',
    description='Input variables for xclim indicators',
    kinds={'Xarray', 'Zarr'},
    key_prefix=['variables'],
    io_manager_key='xarray_zarr_io_manager',
    dagster_type=ParameterDagsterType,
    metadata={
        **ParameterDagsterType.metadata,
        # 'cluster/processes': True,
        # 'cluster/n_workers': 1,
        # 'cluster/threads_per_worker': 8,
        # 'cluster/memory_limit': '75GB',
    },
    templates=templates_by_output_name.values(),
)
def assets(
    context: AssetExecutionContext,
    **ins_kwargs: xr.Dataset,
) -> Output[xr.Dataset]:
    name = context.asset_key.path[-1]
    compute_fn = getattr(variables, name)
    da_kwargs = datasets_to_data_arrays(ins_kwargs)
    result: xr.Dataset = (
        compute_fn(**da_kwargs).to_dataset().bounds.add().chunk(CHUNKS_TIME)
    )
    return Output(
        value=result, metadata=generate_dataset_asset_metadata(result)
    )
