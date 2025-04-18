from collections.abc import Callable

import xarray as xr
from climatopic_xarray.frequencies import Frequency
from dagster import (
    AssetExecutionContext,
    MultiPartitionsDefinition,
    Output,
)
from xclim.core.indicator import Indicator

from climatopic.climate.variables import (
    templates_by_output_name as input_templates,
)
from climatopic.core.asset_factory import AssetTemplate, asset_factory
from climatopic.core.constants import CHUNKS_TIME
from climatopic.core.dagster_types import IndicatorDagsterType
from climatopic.core.indicators import (
    indicators,
)
from climatopic.core.metadata.metadata import generate_dataset_asset_metadata
from climatopic.core.partitions import (
    frequency_partitions_def,
    precipitation_baseline_partitions_def,
    temperature_baseline_partitions_def,
)
from climatopic.core.utilities import (
    datasets_to_data_arrays,
    get_input_names,
    get_partition_key,
    keywords_to_tags,
)


def _get_baseline_partitions_def(indicator: Indicator):
    if (
        'precipitation' in indicator.keywords
        and 'temperature' not in indicator.keywords
    ):
        return precipitation_baseline_partitions_def
    else:
        return temperature_baseline_partitions_def


def get_partitions_def(indicator: Indicator):
    if 'relative' in indicator.keywords:
        return MultiPartitionsDefinition(
            {
                'frequency': frequency_partitions_def,
                'baseline': _get_baseline_partitions_def(indicator),
            }
        )
    else:
        return frequency_partitions_def


templates_by_output_name = {
    name: AssetTemplate(
        key=name,
        description=indicator.title,
        ins={
            input_name: input_templates[input_name].to_in(
                key_prefix='variables',
            )
            for input_name in get_input_names(indicator)
        },
        partitions_def=get_partitions_def(indicator),
        tags=keywords_to_tags(indicator.keywords),
        metadata={
            key: str(value)
            for key, value in indicator.injected_parameters.items()
        },
    )
    for name, indicator in indicators.iter_indicators()
}


@asset_factory(
    name='indicators',
    description='Climate indicators',
    kinds={'Xarray', 'Zarr'},
    key_prefix=['indicators'],
    io_manager_key='xarray_zarr_io_manager',
    dagster_type=IndicatorDagsterType,
    metadata=IndicatorDagsterType.metadata,
    templates=templates_by_output_name.values(),
)
def assets(
    context: AssetExecutionContext, **ins_kwargs: xr.Dataset
) -> Output[xr.Dataset]:
    name = context.asset_key.path[-1]
    indicator: Callable[..., xr.DataArray] = getattr(indicators, name)
    input_kwargs = datasets_to_data_arrays(ins_kwargs)
    frequency = get_partition_key(context=context, dimension='frequency')
    freq = Frequency(frequency.upper()).value
    ds: xr.Dataset = (
        indicator(**input_kwargs, freq=freq)
        .to_dataset()
        .bounds.add()
        .chunk(CHUNKS_TIME)
    )
    return Output(
        value=ds,
        metadata=generate_dataset_asset_metadata(ds),
    )
