from collections.abc import Callable, Mapping

import xarray as xr
from dagster import (
    DagsterType,
    PythonObjectDagsterType,
    TypeCheck,
    TypeCheckContext,
    check_dagster_type,
)
from xarray_schema import DatasetSchema, SchemaError

from dagster_xarray.metadata import dataset_schema_to_metadata

DatasetDagsterType = PythonObjectDagsterType(
    python_type=xr.Dataset,
    name='DatasetDagsterType',
    description='An xarray.Dataset object',
)


def xarray_dataset_schema_to_dagster_type_check_fn(
    schema: DatasetSchema,
) -> Callable[[TypeCheckContext, object], TypeCheck]:
    def _type_check_fn(
        context: TypeCheckContext,
        value: object,
    ) -> TypeCheck:
        type_check: TypeCheck = check_dagster_type(
            dagster_type=DatasetDagsterType, value=value
        )

        if not type_check.success:
            return type_check

        assert isinstance(value, xr.Dataset)

        try:
            schema.validate(value)
        except SchemaError as error:
            type_check = TypeCheck(
                success=False,
                description=f'Schema validation failed: {error}',
            )
        except Exception as error:
            type_check = TypeCheck(
                success=False,
                description=f'Unexpected error during validation: {error}',
            )
        else:
            type_check = TypeCheck(success=True)

        return type_check

    return _type_check_fn


def xarray_dataset_schema_to_dagster_type(
    schema: DatasetSchema | dict,
    *,
    name: str | None = None,
    description: str | None = None,
    metadata: dict | None = None,
) -> DagsterType:
    if isinstance(schema, Mapping):
        schema = DatasetSchema.from_json(schema)
    metadata = (
        dataset_schema_to_metadata(schema) if metadata is None else metadata
    )
    return DagsterType(
        name=name,
        description=description,
        type_check_fn=xarray_dataset_schema_to_dagster_type_check_fn(schema),
        typing_type=xr.Dataset,
        metadata=metadata,
    )
