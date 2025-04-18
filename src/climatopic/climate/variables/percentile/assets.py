from collections.abc import Callable

import xarray as xr
from dagster import (
    AssetExecutionContext,
    Output,
    StaticPartitionsDefinition,
)

from climatopic.climate.variables.primary import (
    templates_by_output_name as input_templates_by_output_name,
)
from climatopic.core.asset_factory import AssetTemplate, asset_factory
from climatopic.core.constants import CHUNKS_PERCENTILE
from climatopic.core.dagster_types import DoyPercentileParameterDagsterType
from climatopic.core.metadata.metadata import generate_dataset_asset_metadata
from climatopic.core.utilities import (
    dataset_to_data_array,
    generate_baselines,
    get_input_names,
    keywords_to_tags,
)
from climatopic.core.variables import variables

templates_by_output_name: dict[str, AssetTemplate] = {
    name: AssetTemplate(
        key=name,
        description=variable.title,
        ins={
            input_name: input_templates_by_output_name[input_name].to_in(
                key_prefix=['variables']
            )
            for input_name in get_input_names(variable)
        },
        partitions_def=StaticPartitionsDefinition(generate_baselines(1961)),
        tags=keywords_to_tags(variable.keywords),
        metadata={
            key: str(value)
            for key, value in variable.injected_parameters.items()
        },
    )
    for name, variable in variables.filter_variables(keywords=['percentile'])
}


@asset_factory(
    name='doy percentiles',
    description='Day-of-year percentile variables for xclim indicators',
    kinds={'Xarray', 'Zarr'},
    key_prefix=['variables'],
    io_manager_key='xarray_zarr_io_manager',
    templates=templates_by_output_name.values(),
    dagster_type=DoyPercentileParameterDagsterType,
    metadata={**DoyPercentileParameterDagsterType.metadata},
)
def assets(
    context: AssetExecutionContext, **ins_kwargs: xr.Dataset
) -> Output[xr.Dataset]:
    name: str = context.asset_key_for_output().path[-1]
    compute_fn: Callable[..., xr.DataArray] = getattr(variables, name)
    baseline: list[str] = context.partition_key.split('-')
    reference_period: tuple[str, str] = (baseline[0], baseline[1])
    # All percentile variables have a single input dataset
    ds: xr.Dataset = next(iter(ins_kwargs.values()))
    da: xr.DataArray = dataset_to_data_array(ds)
    # TODO (mike): Add bounds. Time bounds should reflect the window size.
    result: xr.Dataset = (
        compute_fn(da, reference_period=reference_period)
        .chunk(CHUNKS_PERCENTILE)
        .to_dataset()
    )
    return Output(
        value=result, metadata=generate_dataset_asset_metadata(result)
    )
