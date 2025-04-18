from collections.abc import Callable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, cast

import xarray as xr
import yaml
from dagster import AssetExecutionContext, MultiPartitionKey
from xclim.core.indicator import Indicator, Parameter
from xclim.core.units import convert_units_to
from xclim.core.utils import InputKind

from climatopic.core.types import TChunkDim, TChunks

ROOT_DIR: Path = Path(__file__).parent.parent.absolute()

DATA_DIR: Path = ROOT_DIR / 'data'


def load_yaml(path: Path | str) -> Any:
    """Load a YAML file"""
    with open(path, 'r') as f:
        return yaml.safe_load(f)


def open_mf_dataset(
    path: Path | str,
    *,
    chunks: TChunks = None,
    concat_dim: str = 'time',
    preprocess: Callable[[xr.Dataset], xr.Dataset] | None = None,
    sort: bool | Callable[[Path], Any] | None = None,
    **kwargs: Any,
) -> xr.Dataset:
    """Consolidates multiple netCDF files along a specified dimension.

    This function is a wrapper around `xarray.open_mfdataset`
    with sensible defaults for multi-file datasets separated by a common
    dimension.

    Variables that contain the `concat_dim` dimension are concatenated along
    the common dimension. Variables that do not contain the `concat_dim`
    dimension are taken from the first dataset.

    Parameters
    ----------
    path : Path | str
        Path to the directory containing the netCDF files.
    chunks : Chunks, optional
        Per-file chunk size, by default `None`.
        This chunking is applied per-file, not to the final dataset.
    concat_dim : str, optional
        Dimension to concatenate the variables along, by default "time".
    preprocess : Callable[[xr.Dataset], xr.Dataset], optional
        Function to apply to each file before concatenating, by default None.
    sort : bool | Callable[[Path], Any] | None, optional
        Sort the files before concatenating, by default None.
        If `True`, files are sorted by name.
        If a callable, it should return a value to sort the files by.
    **kwargs : Any
        Keyword arguments to pass to `xarray.open_dataset()`
    """
    # TODO: (mike) Should this use a context manager...?
    paths = list(Path(path).glob('*.nc'))
    if sort is not None:
        key: Callable[[Path], Any] | None = sort if callable(sort) else None
        paths: list[Path] = sorted(paths, key=key)
    return xr.open_mfdataset(
        paths=paths,
        chunks=chunks,
        combine='nested',
        combine_attrs='override',
        compat='override',
        concat_dim=concat_dim,
        coords='minimal',
        data_vars='minimal',
        preprocess=preprocess,
        **kwargs,
    ).chunk(-1)


def datasets_to_data_arrays(
    datasets: Mapping[str, xr.Dataset],
) -> dict[str, xr.DataArray]:
    """Convert a dictionary of datasets with a single data variable to a dictionary of data arrays."""
    return {
        name: dataset_to_data_array(dataset)
        for name, dataset in datasets.items()
    }


def dataset_to_data_array(
    dataset: xr.Dataset,
    name: str | None = None,
) -> xr.DataArray:
    """Convert a Dataset with a single data variable to a DataArray.

    Parameters
    ----------
    dataset : xr.Dataset
        The input Dataset.
    name : str, optional
        The new name of the output DataArray.

    Returns
    -------
    xr.DataArray
        The output DataArray.

    Raises
    ------
    ValueError
        If the input Dataset does not have exactly one data variable.
    """
    if len(dataset.data_vars) != 1:
        raise ValueError(
            'dataset must have exactly one data variable, '
            f'found {dataset.data_vars.keys()}'
        )

    data_array: xr.DataArray = next(iter(dataset.data_vars.values()))

    if name is not None:
        data_array.name = name

    return data_array


def validate_chunks(
    obj: xr.Dataset | xr.DataArray,
    chunks: bool | TChunks,
) -> None:
    """Validate chunk specifications for xarray objects.

    Parameters
    ----------
    obj : xr.Dataset | xr.DataArray
        The input object to validate.
    chunks : bool | dict[str, int | str | tuple[int, ...]]
        The expected chunk specification.
        If `True`, the object must be chunked.

    Raises
    ------
    ValueError
        If the object does not match the expected chunk specification
    """
    has_chunks: bool = obj.chunksizes != {}

    if isinstance(chunks, bool):
        if chunks != has_chunks:
            state = 'un' if not chunks else ''
            raise ValueError(f'expected a {state}chunked object')
        return

    if not isinstance(chunks, dict):
        raise TypeError('chunks must be bool or dict')

    if not has_chunks:
        raise ValueError('expected a chunked object')

    if not chunks:
        raise ValueError('empty chunks dictionary')

    # Validate each dimension
    for dim, expected in chunks.items():
        if dim not in obj.chunksizes:
            raise ValueError(f'expected chunks along dimension {dim}')

        actual: tuple[int, ...] = obj.chunksizes[dim]

        try:
            _validate_chunk_spec(
                expected=expected, actual=actual, dim_size=obj.sizes[dim]
            )
        except ValueError as e:
            raise ValueError(f'invalid chunks for dimension {dim}: {str(e)}')


def _validate_chunk_spec(
    expected: TChunkDim,
    actual: tuple[int, ...],
    dim_size: int,
) -> None:
    """Validate chunk specification for a single dimension."""
    match expected:
        case 'auto':
            return
        case -1:
            if actual != (dim_size,):
                raise ValueError(f'Expected single chunk of size {dim_size}')
        case int() if expected > 0:
            if not all(chunk == expected for chunk in actual[:-1]):
                raise ValueError(f'Expected chunks of size {expected}')
        case tuple() if all(isinstance(x, int) for x in expected):
            if expected != actual:
                raise ValueError(f'Expected exact chunks {expected}')
        case _:
            raise ValueError(f'Invalid chunk specification: {expected}')


def get_partition_key_by_dimension(
    context: AssetExecutionContext,
    *,
    dimension: str,
) -> str:
    """Get the partition key for a given dimension of a multi-partition key.

    Parameters
    ----------
    context : AssetExecutionContext
        The asset execution context.
    dimension : str
        The dimension for which to get the partition key.

    Returns
    -------
    str
        The partition key for the given dimension.
    """
    partition_key = context.partition_key
    if not isinstance(partition_key, MultiPartitionKey):
        raise ValueError('Partition key is not a multi-partition key')
    keys_by_dimension: Mapping[str, str] = partition_key.keys_by_dimension
    if dimension not in keys_by_dimension:
        raise ValueError(f"Partition dimension '{dimension}' not found")
    return keys_by_dimension[dimension]


def get_partition_key(
    context: AssetExecutionContext,
    dimension: str | None = None,
) -> str:
    if isinstance(context.partition_key, MultiPartitionKey):
        if dimension is None:
            raise ValueError('dimension is required for multi-partition keys')
        return get_partition_key_by_dimension(context, dimension=dimension)
    return context.partition_key


def convert_units(
    source: xr.DataArray,
    target: xr.DataArray | str,
    context: Literal['infer', 'hydro', 'none'] | None = None,
) -> xr.DataArray:
    """Convert the units of a DataArray to the specified units."""
    return cast(
        xr.DataArray,
        convert_units_to(source=source, target=target, context=context),
    )


def keywords_to_tags(
    keywords: str | None, separator: str = ' '
) -> dict[str, Literal['true']]:
    """Convert a string of keywords to a Dagster tags mapping."""
    if keywords is None:
        return {}
    true_literal: Literal['true'] = 'true'
    return {
        keyword: true_literal for keyword in keywords.strip().split(separator)
    }


def generate_baselines(start: int, length: int = 30, interval: int = 10):
    end = datetime.now().year
    end -= end % 10
    return [
        '{}-{}'.format(*baseline)
        for baseline in zip(
            range(start, end, interval),
            range(start + length - 1, end + 1, interval),
        )
    ]


def get_input_names(indicator: Indicator) -> list[str]:
    """Get the input variables for an `xclim` indicator class.

    Returns the names of the parameters with `InputKind.VARIABLE`.

    Parameters
    ----------
    indicator : Indicator
        The `xclim` indicator class.

    Returns
    -------
    list[str]
        The names of the input variables.
    """
    parameters: dict[str, Parameter] = indicator.parameters
    return [
        key
        for key, value in parameters.items()
        if value.kind == InputKind.VARIABLE
    ]


def clamp_wrap(value: int, min: int, max: int) -> int:
    """Wrap a value around a range.

    Parameters
    ----------
    value : int
        The value to wrap
    min : int
        The minimum (inclusive) value of the range
    max : int
        The maximum (exclusive) value of the range

    Returns
    -------
    int
        The wrapped value

    Examples
    --------
    ```python
    >>> clamp_wrap(5, 0, 10)
    5
    >>> clamp_wrap(15, 0, 10)
    5
    ```
    """
    return min + (value - min) % (max - min)
