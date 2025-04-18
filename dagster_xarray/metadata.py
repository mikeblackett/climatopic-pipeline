from collections.abc import Sequence
from typing import Any, Dict, Hashable, Mapping

import pandas as pd
from dagster import MarkdownMetadataValue, MetadataValue
from xarray_schema import (
    ChunksSchema,
    CoordsSchema,
    DTypeSchema,
    DataArraySchema,
    DatasetSchema,
    DimsSchema,
    NameSchema,
    ShapeSchema,
)

type TSchema = (
    ChunksSchema | DimsSchema | DTypeSchema | NameSchema | ShapeSchema
)

__all__ = [
    'dataset_schema_to_metadata',
    'data_array_schema_to_metadata',
]


def dataset_schema_to_metadata(
    schema: DatasetSchema,
) -> dict[str, MetadataValue]:
    coords: CoordsSchema | None = schema.coords
    data_vars: Dict[Hashable, DataArraySchema | None] | None = schema.data_vars

    metadata: dict[str, MetadataValue] = {}

    if coords is not None:
        metadata['Coordinates'] = _coords_schema_to_metadata(coords)

    if data_vars is not None:
        metadata['Variables'] = _data_vars_schema_to_metadata(data_vars)

    return metadata


def data_array_schema_to_metadata() -> None:
    raise NotImplementedError()


def _summarize_data_array_schema(
    schema: DataArraySchema, keys: Sequence[str] | None = None
) -> dict[str, Any]:
    keys = keys or ('name', 'dims', 'dtype', 'shape')
    json: dict = schema.json
    return {key: json[key] for key in keys if key in json}


def _coords_schema_to_metadata(schema: CoordsSchema) -> MarkdownMetadataValue:
    coords: dict[Hashable, dict[str, Any]] = {
        name: _summarize_data_array_schema(data_array_schema)
        for name, data_array_schema in schema.coords.items()
        if data_array_schema is not None
    }
    coords_md: str = _to_table(coords)
    return MetadataValue.md(coords_md)


def _data_vars_schema_to_metadata(
    schema: Mapping[Hashable, DataArraySchema | None],
) -> MarkdownMetadataValue:
    data_vars: dict[Hashable, dict[str, Any]] = {
        name: _summarize_data_array_schema(data_array_schema)
        for name, data_array_schema in schema.items()
        if data_array_schema is not None
    }
    data_vars_md: str = _to_table(data_vars)
    return MetadataValue.md(data_vars_md)


def _to_table(metadata: dict):
    return pd.DataFrame.from_dict(data=metadata, orient='index').to_markdown(
        index=False
    )
