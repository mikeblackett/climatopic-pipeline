from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

import xarray as xr
from xclim.core import Quantified
from xclim.core.calendar import percentile_doy
from xclim.core.units import convert_units_to, declare_units


class PercentileIndice(Protocol):
    def __call__(
        self,
        *args,
        percentiles: int | Sequence[int] | None = None,
        window: int | None = None,
        reference_period: tuple[str, str] | None = None,
    ) -> xr.DataArray: ...


@declare_units(tas='[temperature]')
def tas_per(
    tas: xr.DataArray,
    *,
    percentiles: int | Sequence[int] = (10, 25, 75, 90),
    window: int = 5,
    reference_period: tuple[str, str] | None = None,
) -> xr.DataArray:
    """Day-of-year percentiles of daily mean temperature.

    Percentiles of daily mean air temperature, calculated over the
    specified reference period using a rolling window centered on each
    calendar day.

    Parameters
    ----------
    tas : xr.DataArray
        Daily mean temperature data.
    percentiles : int or sequence of int, optional
        Percentiles to calculate. Defaults to (10, 25, 75, 90).
    window : int, optional
        Window size for the rolling window. Defaults to 5.
    reference_period : tuple of str, optional
        Reference period for the calculation. Defaults to None.


    Returns
    -------
    xr.DataArray
        Percentiles of daily mean temperature calculated over the
        {reference_period} using a rolling {window}-day window centered on
        each calendar day. [degK]
    """
    if reference_period:
        tas = tas.sel(time=slice(*reference_period))
    return (
        percentile_doy(tas, window=window, per=percentiles)
        .rename('tas_per')
        .transpose('dayofyear', ...)
    )


@declare_units(pr='[precipitation]', thresh='[precipitation]')
def pr_per(
    pr: xr.DataArray,
    *,
    percentiles: int | Sequence[int] = (10, 25, 75, 90),
    window: int = 5,
    reference_period: tuple[str, str] | None = None,
    thresh: Quantified = '1 mm/day',
) -> xr.DataArray:
    """Day-of-year percentiles of daily precipitation flux.

    Percentiles of daily precipitation flux, calculated over the
    specified reference period using a rolling window centered on each
    calendar day.

    Parameters
    ----------
    pr : xr.DataArray
        Daily precipitation flux data.
    percentiles : int or sequence of int, optional
        Percentiles to calculate. Defaults to (10, 25, 75, 90).
    window : int, optional
        Window size for the rolling window. Defaults to 5.
    reference_period : tuple of str, optional
        Reference period for the calculation. Defaults to None.


    Returns
    -------
    xr.DataArray
        Percentiles of precipitation flux calculated over the
        {reference_period} using a rolling {window}-day window centered on
        each calendar day. [degK]
    """
    if reference_period:
        pr = pr.sel(time=slice(*reference_period))
    thresh_ = convert_units_to(thresh, pr, context='hydro')
    above = pr.where(pr > thresh_)
    return (
        percentile_doy(above, window=window, per=percentiles)
        .rename('pr_per')
        .transpose('dayofyear', ...)
    )


@declare_units(tasmax='[temperature]')
def tasmax_per(
    tasmax: xr.DataArray,
    *,
    percentiles: int | Sequence[int] = (10, 25, 75, 90),
    window: int = 5,
    reference_period: tuple[str, str] | None = None,
) -> xr.DataArray:
    """Day-of-year percentiles of daily maximum temperature.

    Percentiles of daily maximum air temperature, calculated over the
    specified reference period using a rolling window centered on each
    calendar day.

    Parameters
    ----------
    tasmax : xr.DataArray
        Daily maximum temperature data.
    percentiles : int or sequence of int, optional
        Percentiles to calculate. Defaults to (10, 25, 75, 90).
    window : int, optional
        Window size for the rolling window. Defaults to 5.
    reference_period : tuple of str, optional
        Reference period for the calculation. Defaults to None.


    Returns
    -------
    xr.DataArray
        Percentiles of daily maximum temperature calculated over the
        {reference_period} using a rolling {window}-day window centered on
        each calendar day. [degK]
    """
    if reference_period:
        tasmax = tasmax.sel(time=slice(*reference_period))
    return (
        percentile_doy(tasmax, window=window, per=percentiles)
        .rename('tasmax_per')
        .transpose('dayofyear', ...)
    )


@declare_units(tasmin='[temperature]')
def tasmin_per(
    tasmin: xr.DataArray,
    *,
    percentiles: int | Sequence[int] = (10, 25, 75, 90),
    window: int = 5,
    reference_period: tuple[str, str] | None = None,
) -> xr.DataArray:
    """Day-of-year percentiles of daily minimum temperature.

    Percentiles of daily minimum air temperature, calculated over the
    specified reference period using a rolling window centered on each
    calendar day.

    Parameters
    ----------
    tasmin : xr.DataArray
        Daily minimum temperature data.
    percentiles : int or sequence of int, optional
        Percentiles to calculate. Defaults to (10, 25, 75, 90).
    window : int, optional
        Window size for the rolling window. Defaults to 5.
    reference_period : tuple of str, optional
        Reference period for the calculation. Defaults to None.


    Returns
    -------
    xr.DataArray
        Percentiles of daily minimum temperature calculated over the
        {reference_period} using a rolling {window}-day window centered on
        each calendar day. [degK]
    """
    if reference_period:
        tasmin = tasmin.sel(time=slice(*reference_period))
    return (
        percentile_doy(tasmin, window=window, per=percentiles)
        .rename('tasmin_per')
        .transpose('dayofyear', ...)
    )
