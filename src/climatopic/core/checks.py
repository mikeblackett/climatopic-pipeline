from typing import Any

import xarray as xr
from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetChecksDefinition,
    AssetKey,
    asset_check,
)
from xclim.core import ValidationError
from xclim.core.cfchecks import cfcheck_from_name
from xclim.core.datachecks import check_freq

from climatopic.core.types import TChunks
from climatopic.core.utilities import dataset_to_data_array, validate_chunks


def build_xarray_frequency_check(
    asset: AssetKey | str,
    *,
    freq: str,
) -> AssetChecksDefinition:
    """Return an AssetChecksDefinition that checks the temporal frequency of a
     xarray object asset.

    Parameters
    ----------
    asset : AssetKey | str
        The key describing the asset to check.
    freq : str
        The expected frequency, using pandas frequency strings.

    Returns
    -------
    AssetChecksDefinition
        The AssetChecksDefinition that checks the frequency of the asset.
    """
    # TODO: This should be replaced by @multi_asset_check once there is a way to pass the asset output to the check function.
    # See: https://github.com/dagster-io/dagster/issues/21772

    @asset_check(
        name='check_frequency',
        description=(
            'Check that the dataset has the correct temporal frequency.'
        ),
        asset=asset,
        blocking=True,
        metadata={'frequency': freq},
    )
    def _check(
        context: AssetCheckExecutionContext,
        dataset: xr.Dataset,
    ) -> AssetCheckResult:
        data_array: xr.DataArray = dataset_to_data_array(dataset)
        try:
            check_freq(data_array, freq=freq, strict=True)
            passed = True
            metadata: dict[str, str] = {'frequency': freq}
        except ValidationError as error:
            passed = False
            metadata = {'error': error.msg}
        except Exception as error:
            passed = False
            metadata = {'error': str(error)}
        return AssetCheckResult(
            passed=passed,
            severity=AssetCheckSeverity.ERROR,
            metadata=metadata,
        )

    return _check


def build_xarray_cf_attributes_check(
    asset: AssetKey | str,
    *,
    attrs: list[str] | None = None,
) -> AssetChecksDefinition:
    """Return an AssetChecksDefinition that checks the CF attributes of a dataset.

    Parameters
    ----------
    asset : AssetKey | str
        The key describing the asset to check.
    attrs : list[str], optional
        A list of CF attributes to check for, by default ['cell_methods', 'standard_name'].
    """

    if attrs is None:
        attrs = ['cell_methods', 'standard_name']

    @asset_check(
        name='check_cf_attrs',
        description='Check that the dataset has the correct CF attributes.',
        asset=asset,
        blocking=True,
    )
    def _check(
        context: AssetCheckExecutionContext,
        dataset: xr.Dataset,
    ) -> AssetCheckResult:
        data_array: xr.DataArray = dataset_to_data_array(dataset)
        try:
            cfcheck_from_name(
                varname=str(data_array.name),
                vardata=data_array,
                attrs=attrs,
            )
            passed = True
            metadata: dict[str, str] = {
                name: data_array.attrs[name] for name in attrs
            }
        except ValidationError as e:
            passed = False
            metadata = {'error': e.msg}
        except Exception as e:
            passed = False
            metadata = {'error': str(e)}

        return AssetCheckResult(
            passed=passed,
            severity=AssetCheckSeverity.WARN,
            metadata=metadata,
        )

    return _check


def build_xarray_chunk_check(
    asset: AssetKey | str,
    *,
    chunks: bool | TChunks,
) -> AssetChecksDefinition:
    """Return an AssetChecksDefinition that checks the chunking of an
     xarray object asset.

    Parameters
    ----------
    asset : AssetKey | str
        The key describing the asset to check.
    chunks : bool | dict[str, int | str | tuple[int, ...]]
        The expected chunking. Can be any value accepted by the xarray `chunk` method.

    Returns
    -------
    AssetChecksDefinition
        The AssetChecksDefinition that checks the chunking of the asset.
    """

    @asset_check(
        name='check_chunks',
        description='Check that the dataset has the correct chunking.',
        asset=asset,
        blocking=True,
    )
    def _check(
        context: AssetCheckExecutionContext,
        dataset: xr.Dataset,
    ) -> AssetCheckResult:
        data_array: xr.DataArray = dataset_to_data_array(dataset)
        try:
            validate_chunks(data_array, chunks)
            passed = True
            metadata: dict[str, Any] = {'chunks': chunks}
        except ValueError as error:
            passed = False
            metadata = {'error': str(error)}
        return AssetCheckResult(
            passed=passed,
            severity=AssetCheckSeverity.WARN,
            metadata=metadata,
        )

    return _check


# def build_data_flag_asset_check(
#     asset: AssetKey | str,
# ) -> AssetChecksDefinition:
#     @asset_check(
#         name='check_data_flags',
#         description=(
#             'Evaluate the supplied DataArray for a set of data flag checks.'
#         ),
#         asset=asset,
#         blocking=False,
#     )
#     def _check_data_flags(
#         context: AssetCheckExecutionContext,
#         dataset: xr.Dataset,
#     ) -> AssetCheckResult:
#         flags: dict = spec.metadata.get('data_flags', {})
#         if flags == {}:
#             return AssetCheckResult(
#                 passed=True,
#                 severity=AssetCheckSeverity.WARN,
#                 metadata=flags,
#             )

#         data_array: xr.DataArray = dataset_to_dataarray(dataset)
#         try:
#             data_flags(
#                 data_array,
#                 flags=flags,
#                 raise_flags=True,
#             )
#             passed = True
#             metadata = {'data_flags': flags}
#         except DataQualityException as e:
#             passed = False
#             metadata = {'message': e.message}
#         except Exception as e:
#             passed = False
#             metadata = {'message': str(e)}

#         return AssetCheckResult(
#             passed=passed,
#             severity=AssetCheckSeverity.WARN,
#             metadata=metadata,
#         )

#     return _check_data_flags
