from typing import Any, Hashable, Mapping

import xarray as xr
from tabulate import tabulate

__all__ = [
    'format_chunk_sizes',
    'format_coords',
    'format_data_vars',
    'format_dims',
    'format_size',
    'format_task_graph_size',
]

TABLE_FORMAT = 'github'


def format_dims(
    dims: Mapping[Hashable, int],
    dims_with_indexes: Mapping[Hashable, xr.Index],
) -> str:
    # Dimensions with indexes (i.e. coordinates) are formatted in bold
    dim_names_map = {
        dim: f'**{dim}**' if dim in dims_with_indexes else dim for dim in dims
    }
    return tabulate(
        [[dim_names_map[dim], size] for dim, size in dims.items()],
        headers=['Dimension', 'Size'],
        tablefmt=TABLE_FORMAT,
    )


def format_sizes(sizes: Mapping[Hashable, int]) -> str:
    return tabulate(
        [[dim, size] for dim, size in sizes.items()],
        headers=['Dimension', 'Length'],
        tablefmt=TABLE_FORMAT,
    )


def format_chunk_sizes(chunksizes: Mapping[Hashable, tuple[int, ...]]) -> str:
    chunks = _summarize_chunksizes(chunksizes)
    return tabulate(
        [[dim, chunk] for dim, chunk in chunks.items()],
        headers=['Dimension', 'Size'],
        tablefmt=TABLE_FORMAT,
    )


def dask_task_graph_size(da: xr.DataArray) -> int | None:
    if da.chunks is None:
        return None
    graph = da.__dask_graph__()
    if graph is None:
        return None
    return len(graph)  # type: ignore


def format_task_graph_size(obj: xr.Dataset | xr.DataArray) -> int | None:
    if obj.chunks is None:
        return None
    graph = obj.__dask_graph__()
    if graph is None:
        return None
    return len(graph)


def _format_variables(variables: Mapping[Hashable, xr.DataArray], dims) -> str:
    variables_summary = _summarize_variables(variables)
    return tabulate(
        [
            [
                f'**{key}**' if key in dims else key,
                value['dims'],
                value['dtype'],
                value['chunks'],
            ]
            for key, value in variables_summary.items()
        ],
        headers=['Variable', 'Dimensions', 'Dtype', 'Chunks'],
        tablefmt=TABLE_FORMAT,
    )


def format_coords(coords: xr.Coordinates, dims) -> str:
    return _format_variables(coords, dims)


def format_data_vars(data_vars: Mapping[Hashable, xr.DataArray], dims) -> str:
    return _format_variables(data_vars, dims)


def format_size(
    nbytes: int,
    precision: int = 2,
) -> str:
    if nbytes < 1e6:
        size = nbytes / 1e3
        unit = 'KB'
    elif nbytes < 1e9:
        size = nbytes / 1e6
        unit = 'MB'
    else:
        size = nbytes / 1e9
        unit = 'GB'

    return f'{size:.{precision}f} {unit}'


def _summarize_chunks(chunks: tuple[tuple[int, ...], ...]) -> tuple[int, ...]:
    return tuple([max(chunk) for chunk in chunks])


def _summarize_chunksizes(
    chunksizes: Mapping[Hashable, tuple[int, ...]],
) -> dict[str, int]:
    return {str(name): max(size) for name, size in chunksizes.items()}


def _summarize_variable(variable: xr.DataArray) -> dict[str, Any]:
    return {
        'dims': ', '.join([str(dim) for dim in variable.dims]),
        'dtype': variable.dtype.name,  # type: ignore
        'chunks': (
            _summarize_chunks(variable.chunks) if variable.chunks else None
        ),
    }


def _summarize_variables(
    variables: Mapping[Hashable, xr.DataArray],
) -> dict[str, dict[str, Any]]:
    return {
        str(key): _summarize_variable(value)
        for key, value in variables.items()
    }
