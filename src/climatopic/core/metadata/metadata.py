from typing import Any, Hashable

import xarray as xr
from dagster import MarkdownMetadataValue, MetadataValue, TextMetadataValue

from climatopic.core.utilities import dataset_to_data_array
from .format import (
    format_chunk_sizes,
    format_dims,
    format_size,
    format_task_graph_size,
)


def xarray_nbytes_to_metadata(
    obj: xr.Dataset | xr.DataArray,
) -> TextMetadataValue:
    size_mb: str = format_size(obj.nbytes)
    return MetadataValue.text(size_mb)


def xarray_sizes_to_metadata(
    obj: xr.Dataset | xr.DataArray,
) -> MarkdownMetadataValue:
    shape: str = format_dims(obj.sizes, obj.indexes)  # type: ignore
    return MetadataValue.md(shape)


def xarray_chunksizes_to_metadata(
    obj: xr.Dataset | xr.DataArray,
) -> MarkdownMetadataValue:
    chunks: str = format_chunk_sizes(obj.chunksizes)
    return MetadataValue.md(chunks)


def xarray_task_graph_size_to_metadata(
    obj: xr.Dataset | xr.DataArray,
) -> MetadataValue:
    task_graph_size: int | None = format_task_graph_size(obj)
    if task_graph_size is None:
        return MetadataValue.null()
    return MetadataValue.int(task_graph_size)


def data_array_units_to_metadata(data_array: xr.DataArray) -> MetadataValue:
    units: Any | None = data_array.attrs.get('units')
    if units is None:
        return MetadataValue.null()
    return MetadataValue.text(units)


def data_array_name_to_metadata(data_array: xr.DataArray) -> MetadataValue:
    name: Hashable | None = data_array.name
    if name is None:
        return MetadataValue.null()
    return MetadataValue.text(str(name))


def generate_dataset_asset_metadata(
    dataset: xr.Dataset,
) -> dict[str, MetadataValue]:
    data_array: xr.DataArray = dataset_to_data_array(dataset)
    return {
        'Variable name': data_array_name_to_metadata(data_array),
        'Dimensions': xarray_sizes_to_metadata(data_array),
        'Chunks': xarray_chunksizes_to_metadata(data_array),
        'Units': data_array_units_to_metadata(data_array),
        'Size': xarray_nbytes_to_metadata(data_array),
        'Tasks': xarray_task_graph_size_to_metadata(data_array),
    }
