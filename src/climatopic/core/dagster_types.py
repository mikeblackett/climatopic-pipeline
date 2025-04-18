from dagster import DagsterType

from climatopic.core import schema
from dagster_xarray import xarray_dataset_schema_to_dagster_type

HadukDagsterType: DagsterType = xarray_dataset_schema_to_dagster_type(
    schema=schema.haduk_dataset_schema,
    name='HadukDagsterType',
    description=(
        'An xarray Dataset with a single data variable '
        'representing a gridded HadUK-Grid climate variable.'
    ),
)

ParameterDagsterType: DagsterType = xarray_dataset_schema_to_dagster_type(
    schema=schema.climatopic_dataset_schema,
    name='ParameterDagsterType',
    description=(
        'An xarray Dataset with a single data variable '
        'representing a gridded climatopic climate parameter.'
    ),
)

DoyPercentileParameterDagsterType: DagsterType = xarray_dataset_schema_to_dagster_type(
    schema=schema.doy_dataset_schema,
    name='DoyPercentileParameterDagsterType',
    description=(
        'An xarray Dataset with a single data variable '
        'representing gridded day-of-year percentiles for a climatopic climate parameter.'
    ),
)

IndicatorDagsterType: DagsterType = xarray_dataset_schema_to_dagster_type(
    schema=schema.climatopic_dataset_schema,
    name='IndicatorDagsterType',
    description=(
        'An xarray Dataset with a single data variable '
        'representing a gridded climatopic climate indicator.'
    ),
)
