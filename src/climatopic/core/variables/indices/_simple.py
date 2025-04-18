from __future__ import annotations

from typing import cast

import xarray as xr
from xclim import indices
from xclim.core import Quantified
from xclim.core.units import convert_units_to, declare_units

from climatopic.core.utilities import convert_units


@declare_units(tasmin='[temperature]')
def tasmin(tasmin: xr.DataArray, units: str = 'K') -> xr.DataArray:
    """Returns the daily minimum temperature with specified units."""
    return tasmin.pipe(
        convert_units,
        units,
    )


@declare_units(tasmax='[temperature]')
def tasmax(tasmax: xr.DataArray, units: str = 'K') -> xr.DataArray:
    """Returns the daily maximum temperature with specified units."""
    return tasmax.pipe(
        convert_units,
        units,
    )


@declare_units(rainfall='[length]')
def rainfall(rainfall: xr.DataArray, units: str = 'mm') -> xr.DataArray:
    """Returns the daily rainfall with specified units."""
    return rainfall.pipe(
        convert_units,
        units,
    )


@declare_units(tasmin='[temperature]', tasmax='[temperature]')
def tas(tasmin: xr.DataArray, tasmax: xr.DataArray) -> xr.DataArray:
    """Calculates the average temperature from minimum and maximum temperatures.

    Assumes a symmetrical distribution (tas = (tasmax + tasmin) / 2)

    Parameters
    ----------
    tasmin : xarray.DataArray
        Minimum (daily) temperature
    tasmax : xarray.DataArray
        Maximum (daily) temperature

    Returns
    -------
    xarray.DataArray
        Mean (daily) temperature [same units as tasmin]
    """
    tas: xr.DataArray = indices.tas(tasmin=tasmin, tasmax=tasmax).rename('tas')
    return tas


@declare_units(rainfall='[length]', thresh='[precipitation]')
def pr(
    rainfall: xr.DataArray,
    *,
    thresh: Quantified = '0 mm/d',
) -> xr.DataArray:
    """Calculates surface precipitation flux from total precipitation amount.

    Parameters
    ----------
    rainfall : xr.DataArray
      Total precipitation amount (mm).
    thresh : Quantified. Default: "0 mm/d"
      Threshold value for precipitation.

    Returns
    -------
    xr.DataArray
      The surface precipitation flux.
    """
    pr: xr.DataArray = cast(
        xr.DataArray,
        convert_units_to(rainfall, 'kg m-2 s-1', context='hydro'),
    ).rename('pr')

    condition: xr.DataArray | float = convert_units_to(
        thresh, pr, context='hydro'
    )
    return pr.where((pr > condition) | pr.isnull(), 0)
