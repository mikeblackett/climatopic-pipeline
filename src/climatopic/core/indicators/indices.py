from __future__ import annotations

from typing import Literal, cast

import xarray as xr
from xclim.core.bootstrapping import percentile_bootstrap
from xclim.core.calendar import resample_doy
from xclim.core.units import convert_units_to, declare_units, to_agg_units
from xclim.indices import run_length
from xclim.indices.generic import compare, threshold_count, get_op


def declare_percentile(**percentiles_by_name):
    def _inner(func: Callable[..., xr.DataArray]):
        @wraps(func)
        def wrapper(*args, **kwargs):
            bound_args = signature(func).bind(*args, **kwargs)
            bound_args.apply_defaults()
            for name, per in percentiles_by_name.items():
                da = bound_args.arguments[name]
                if not isinstance(da, xr.DataArray):
                    raise ValueError(
                        f'Argument {name} should be an xarray.DataArray, not {type(da)}'
                    )
                per_value = bound_args.arguments[per]
                if not isinstance(per_value, (int, float)):
                    raise ValueError(
                        f'Percentile value should be an int or a float, not {type(per_value)}'
                    )
                bound_args.arguments[name] = da.sel(percentiles=per_value)

            return func(*args, **bound_args.arguments)

        return wrapper

    return _inner


@declare_units(tasmax='[temperature]', tasmax_per='[temperature]')
@percentile_bootstrap
def tx_days_above_doy_thresh(
    tasmax: xr.DataArray,
    tasmax_per: xr.DataArray,
    per_thresh: float = 90,
    freq: str = 'YS',
    bootstrap: bool = False,  # noqa
    op: str = '>',
) -> xr.DataArray:
    r"""Number of days with daily maximum temperature above the specified
    percentile threshold.

    Number of days over period where maximum temperature is above a given
    percentile for that day.

    Parameters
    ----------
    tasmax : xarray.DataArray
        Maximum daily temperature.
    tasmax_per : xarray.DataArray
        Percentiles of daily maximum temperature.
    per_thresh : float
        Percentile threshold.
    freq : str
        Resampling frequency.
    bootstrap : bool
        Flag to run bootstrapping of percentiles. Used by percentile_bootstrap decorator.
        Bootstrapping is only useful when the percentiles are computed on a part of the studied sample.
        This period, common to percentiles and the sample must be bootstrapped to avoid inhomogeneities with
        the rest of the time series.
        Keep bootstrap to False when there is no common period, it would give wrong results
        plus, bootstrapping is computationally expensive.
    op : {">", ">=", "gt", "ge"}
        Comparison operation. Default: ">".

    Returns
    -------
    xarray.DataArray, [time]
        Count of days with daily maximum temperature above the {per_thresh}
        percentile [days].

    Notes
    -----
    The percentiles should be computed for a 5-day window centered on each
    calendar day for a reference period.
    """
    tasmax_per = cast(
        xr.DataArray, convert_units_to(source=tasmax_per, target=tasmax)
    ).sel(percentiles=per_thresh)
    thresh = resample_doy(doy=tasmax_per, arr=tasmax)
    above = threshold_count(
        da=tasmax, op=op, threshold=thresh, freq=freq, constrain=('>', '>=')
    )
    return to_agg_units(out=above, orig=tasmax, op='count')


@declare_units(tasmax='[temperature]', tasmax_per='[temperature]')
@percentile_bootstrap
def tx_days_below_doy_thresh(
    tasmax: xr.DataArray,
    tasmax_per: xr.DataArray,
    per_thresh: float = 10,
    freq: str = 'YS',
    bootstrap: bool = False,  # noqa
    op: str = '<',
) -> xr.DataArray:
    r"""Number of days with daily maximum temperature below a specified
    percentile threshold.

    Number of days over period where maximum temperature is below a given
    percentile for that day.

    Parameters
    ----------
    tasmax : xarray.DataArray
        Maximum daily temperature.
    tasmax_per : xarray.DataArray
        Percentiles of daily maximum temperature.
    per_thresh : float
        Percentile threshold.
    freq : str
        Resampling frequency.
    bootstrap : bool
        Flag to run bootstrapping of percentiles. Used by percentile_bootstrap decorator.
        Bootstrapping is only useful when the percentiles are computed on a part of the studied sample.
        This period, common to percentiles and the sample must be bootstrapped to avoid inhomogeneities with
        the rest of the time series.
        Keep bootstrap to False when there is no common period, it would give wrong results
        plus, bootstrapping is computationally expensive.
    op : {">", ">=", "gt", "ge"}
        Comparison operation. Default: ">".

    Returns
    -------
    xarray.DataArray, [time]
        Count of days with daily maximum temperature below the {per_thresh}
        percentile [days].

    Notes
    -----
    The percentiles should be computed for a 5-day window centered on each
    calendar day for a reference period.
    """
    tasmax_per = cast(
        xr.DataArray, convert_units_to(source=tasmax_per, target=tasmax)
    ).sel(percentiles=per_thresh)
    thresh = resample_doy(doy=tasmax_per, arr=tasmax)
    below = threshold_count(
        da=tasmax, op=op, threshold=thresh, freq=freq, constrain=('<', '<=')
    )
    return to_agg_units(out=below, orig=tasmax, op='count')


@declare_units(tasmin='[temperature]', tasmin_per='[temperature]')
@percentile_bootstrap
def tn_days_above_doy_thresh(
    tasmin: xr.DataArray,
    tasmin_per: xr.DataArray,
    per_thresh: float = 90,
    freq: str = 'YS',
    bootstrap: bool = False,  # noqa
    op: str = '>',
) -> xr.DataArray:
    r"""Number of days with daily minimum temperature above a specified
    percentile threshold.

    Number of days over period where minimum temperature is above a given
    percentile for that day.

    Parameters
    ----------
    tasmin : xarray.DataArray
        Minimum daily temperature.
    tasmin_per : xarray.DataArray
        Percentiles of daily minimum temperature.
    per_thresh : float
        Percentile threshold.
    freq : str
        Resampling frequency.
    bootstrap : bool
        Flag to run bootstrapping of percentiles. Used by percentile_bootstrap decorator.
        Bootstrapping is only useful when the percentiles are computed on a part of the studied sample.
        This period, common to percentiles and the sample must be bootstrapped to avoid inhomogeneities with
        the rest of the time series.
        Keep bootstrap to False when there is no common period, it would give wrong results
        plus, bootstrapping is computationally expensive.
    op : {">", ">=", "gt", "ge"}
        Comparison operation. Default: ">".

    Returns
    -------
    xarray.DataArray, [time]
        Count of days with daily minimum temperature above the {per_thresh}
        percentile [days].

    Notes
    -----
    The percentiles should be computed for a 5-day window centered on each
    calendar day for a reference period.
    """
    tasmin_per = cast(
        xr.DataArray, convert_units_to(source=tasmin_per, target=tasmin)
    ).sel(percentiles=per_thresh)
    thresh = resample_doy(doy=tasmin_per, arr=tasmin)
    above = threshold_count(
        da=tasmin, op=op, threshold=thresh, freq=freq, constrain=('>', '>=')
    )
    return to_agg_units(out=above, orig=tasmin, op='count')


@declare_units(tasmin='[temperature]', tasmin_per='[temperature]')
@percentile_bootstrap
def tn_days_below_doy_thresh(
    tasmin: xr.DataArray,
    tasmin_per: xr.DataArray,
    per_thresh: float = 10,
    freq: str = 'YS',
    bootstrap: bool = False,  # noqa
    op: str = '<',
) -> xr.DataArray:
    r"""Number of days with daily minimum temperature below a specified
    percentile threshold.

    Number of days over period where minimum temperature is below a given
    percentile for that day.

    Parameters
    ----------
    tasmin : xarray.DataArray
        Minimum daily temperature.
    tasmin_per : xarray.DataArray
        Percentiles of daily minimum temperature.
    per_thresh : float
        Percentile threshold.
    freq : str
        Resampling frequency.
    bootstrap : bool
        Flag to run bootstrapping of percentiles. Used by percentile_bootstrap decorator.
        Bootstrapping is only useful when the percentiles are computed on a part of the studied sample.
        This period, common to percentiles and the sample must be bootstrapped to avoid inhomogeneities with
        the rest of the time series.
        Keep bootstrap to False when there is no common period, it would give wrong results
        plus, bootstrapping is computationally expensive.
    op : {">", ">=", "gt", "ge"}
        Comparison operation. Default: ">".

    Returns
    -------
    xarray.DataArray, [time]
        Count of days with daily minimum temperature below the {per_thresh}
        percentile [days].

    Notes
    -----
    The percentiles should be computed for a 5-day window centered on each
    calendar day for a reference period.
    """
    tasmin_per = cast(
        xr.DataArray, convert_units_to(source=tasmin_per, target=tasmin)
    ).sel(percentiles=per_thresh)
    thresh = resample_doy(doy=tasmin_per, arr=tasmin)
    below = threshold_count(
        da=tasmin, op=op, threshold=thresh, freq=freq, constrain=('<', '<=')
    )
    return to_agg_units(out=below, orig=tasmin, op='count')


@declare_units(pr='[precipitation]', pr_per='[precipitation]')
@percentile_bootstrap
def pr_days_above_doy_thresh(
    pr: xr.DataArray,
    pr_per: xr.DataArray,
    per_thresh: float = 90,
    freq: str = 'YS',
    bootstrap: bool = False,  # noqa
    op: str = '>',
) -> xr.DataArray:
    r"""Number of days with daily precipitation above a given percentile.

    Number of days over period where precipitation is above a given percentile
    for that day.

    Parameters
    ----------
    pr : xarray.DataArray
        Mean daily precipitation flux.
    pr_per : xarray.DataArray
        Percentiles of wet day precipitation flux.
    per_thresh : float
        Percentile threshold over which a day is considered wet.
    freq : str
        Resampling frequency.
    bootstrap : bool
        Flag to run bootstrapping of percentiles. Used by percentile_bootstrap decorator.
        Bootstrapping is only useful when the percentiles are computed on a part of the studied sample.
        This period, common to percentiles and the sample must be bootstrapped to avoid inhomogeneities with
        the rest of the time series.
        Keep bootstrap to False when there is no common period, it would give wrong results
        plus, bootstrapping is computationally expensive.
    op : {">", ">=", "gt", "ge"}
        Comparison operation. Default: ">".

    Returns
    -------
    xr.DataArray, [time]
      Count of days with daily precipitation above {per_thresh} percentile
      [days].
    """
    pr_per = cast(
        xr.DataArray,
        convert_units_to(source=pr_per, target=pr, context='hydro'),
    ).sel(percentiles=per_thresh)
    thresh = resample_doy(pr_per, pr)
    above = threshold_count(pr, op, thresh, freq, constrain=('>', '>='))
    return to_agg_units(above, pr, 'count')


@declare_units(pr='[precipitation]', pr_per='[precipitation]')
@percentile_bootstrap
def pr_days_below_doy_thresh(
    pr: xr.DataArray,
    pr_per: xr.DataArray,
    per_thresh: float = 90,
    freq: str = 'YS',
    bootstrap: bool = False,  # noqa
    op: str = '<',
) -> xr.DataArray:
    r"""Number of days with daily precipitation below a given percentile.

    Number of days over period where precipitation is below a given percentile
    for that day.

     Parameters
     ----------
     pr : xarray.DataArray
         Mean daily precipitation flux.
     pr_per : xarray.DataArray
         Percentiles of wet day precipitation flux.
     per_thresh : float
         Percentile threshold below which a day is considered wet.
     freq : str
         Resampling frequency.
     bootstrap : bool
         Flag to run bootstrapping of percentiles. Used by percentile_bootstrap decorator.
         Bootstrapping is only useful when the percentiles are computed on a part of the studied sample.
         This period, common to percentiles and the sample must be bootstrapped to avoid inhomogeneities with
         the rest of the time series.
         Keep bootstrap to False when there is no common period, it would give wrong results
         plus, bootstrapping is computationally expensive.
     op : {">", ">=", "gt", "ge"}
         Comparison operation. Default: ">".

     Returns
     -------
     xr.DataArray, [time]
       Count of days with daily precipitation below {per_thresh} percentile
       [days].
    """
    pr_per = cast(
        xr.DataArray,
        convert_units_to(source=pr_per, target=pr, context='hydro'),
    ).sel(percentiles=per_thresh)
    thresh = resample_doy(pr_per, pr)
    above = threshold_count(pr, op, thresh, freq, constrain=('<', '<='))
    return to_agg_units(above, pr, 'count')


@declare_units(
    tasmax='[temperature]',
    pr='[precipitation]',
    tasmax_per='[temperature]',
    pr_per='[precipitation]',
)
@percentile_bootstrap
def warm_and_wet_days(
    tasmax: xr.DataArray,
    pr: xr.DataArray,
    tasmax_per: xr.DataArray,
    pr_per: xr.DataArray,
    tasmax_per_thresh: float = 75,
    pr_per_thresh: float = 75,
    freq: str = 'YS',
    bootstrap: bool = False,  # noqa
    op: str = '>',
) -> xr.DataArray:
    r"""Days with warm and wet conditions.

    Number of days when both daily maximum temperature and
    precipitation are above specified percentiles for that day.

    Parameters
    ----------
    tasmax : xarray.DataArray
    pr : xarray.DataArray
        Mean daily precipitation flux.
    tasmax_per : xarray.DataArray
        Percentiles of daily maximum temperature.
    pr_per : xarray.DataArray
        Percentiles of daily (wet-day) precipitation flux.
    tasmax_per_thresh : float
        Percentile threshold for warm days.
    pr_per_thresh : float
        Percentile threshold for wet days.
    freq : str
        Resampling frequency.
    bootstrap : bool
        Flag to run bootstrapping of percentiles. Used by percentile_bootstrap decorator.
        Bootstrapping is only useful when the percentiles are computed on a part of the studied sample.
        This period, common to percentiles and the sample must be bootstrapped to avoid inhomogeneities with
        the rest of the time series.
        Keep bootstrap to False when there is no common period, it would give wrong results
        plus, bootstrapping is computationally expensive.
    op : {">", ">=", "gt", "ge"}
        Comparison operation. Default: ">".

    Returns
    -------
    xr.DataArray, [time]
      Count of days with daily maximum temperature above the
      {tasmax_per_thresh}th percentile and precipitation above the
      {pr_per_thresh}th percentile [days].
    """
    comparator = get_op(op=op, constrain=('>', '>='))

    tasmax_per = cast(
        xr.DataArray,
        convert_units_to(source=tasmax_per, target=tasmax),
    ).sel(percentiles=tasmax_per_thresh)
    tasmax_thresh = resample_doy(tasmax_per, tasmax)
    tasmax_above = comparator(tasmax, tasmax_thresh)

    pr_per = cast(
        xr.DataArray,
        convert_units_to(source=pr_per, target=pr, context='hydro'),
    ).sel(percentiles=pr_per_thresh)
    pr_thresh = resample_doy(doy=pr_per, arr=pr)
    pr_above = comparator(pr, pr_thresh)

    warm_and_dry = xr.ufuncs.logical_and(tasmax_above, pr_above)
    resampled = warm_and_dry.resample(time=freq).sum(dim='time')
    return to_agg_units(out=resampled, orig=tasmax, op='count')


@declare_units(
    tasmax='[temperature]',
    pr='[precipitation]',
    tasmax_per='[temperature]',
    pr_per='[precipitation]',
)
def warm_and_dry_days(
    tasmax: xr.DataArray,
    pr: xr.DataArray,
    tasmax_per: xr.DataArray,
    pr_per: xr.DataArray,
    tasmax_per_thresh: float = 75,
    pr_per_thresh: float = 25,
    freq: str = 'YS',
    bootstrap: bool = False,  # noqa
    op: str = '>',
) -> xr.DataArray:
    r"""Days with warm and dry conditions.

    Number of days when daily maximum temperature is above the specified
    percentile for that day, and precipitation is below the specified
    percentile for that day.

    Parameters
    ----------
    tasmax : xarray.DataArray
        Maximum daily temperature.
    pr : xarray.DataArray
        Mean daily precipitation flux.
    tasmax_per : xarray.DataArray
        Percentiles of daily maximum temperature.
    pr_per : xarray.DataArray
        Percentiles of precipitation flux.
    tasmax_per_thresh : float
        Percentile threshold for warm days.
    pr_per_thresh : float
        Percentile threshold for dry days.
    freq : str
        Resampling frequency.
    bootstrap : bool
        Flag to run bootstrapping of percentiles. Used by percentile_bootstrap decorator.
        Bootstrapping is only useful when the percentiles are computed on a part of the studied sample.
        This period, common to percentiles and the sample must be bootstrapped to avoid inhomogeneities with
        the rest of the time series.
        Keep bootstrap to False when there is no common period, it would give wrong results
        plus, bootstrapping is computationally expensive.
    op : {">", ">=", "gt", "ge"}
        Comparison operation. Default: ">".

    Returns
    -------
    xarray.DataArray, [time]
        Count of days with daily maximum temperature above the
        {tasmax_per_thresh}th percentile and precipitation below the
        {pr_per_thresh}th percentile [days].
    """
    comparator = get_op(op=op, constrain=('>', '>='))

    tasmax_per = cast(
        xr.DataArray,
        convert_units_to(source=tasmax_per, target=tasmax),
    ).sel(percentiles=tasmax_per_thresh)
    tasmax_thresh = resample_doy(tasmax_per, tasmax)
    tasmax_above = comparator(tasmax, tasmax_thresh)

    pr_per = cast(
        xr.DataArray,
        convert_units_to(source=pr_per, target=pr, context='hydro'),
    ).sel(percentiles=pr_per_thresh)
    pr_thresh = resample_doy(doy=pr_per, arr=pr)
    pr_below = comparator(pr, pr_thresh)

    warm_and_dry = xr.ufuncs.logical_and(tasmax_above, pr_below)
    resampled = warm_and_dry.resample(time=freq).sum(dim='time')
    return to_agg_units(out=resampled, orig=tasmax, op='count')


@declare_units(
    tasmax='[temperature]',
    pr='[precipitation]',
    tasmax_per='[temperature]',
    pr_per='[precipitation]',
)
def cool_and_wet_days(
    tasmax: xr.DataArray,
    pr: xr.DataArray,
    tasmax_per: xr.DataArray,
    pr_per: xr.DataArray,
    tasmax_per_thresh: float = 10,
    pr_per_thresh: float = 90,
    freq: str = 'YS',
    bootstrap: bool = False,  # noqa
    op: str = '<',
) -> xr.DataArray:
    r"""Days with cool and wet conditions.

    Number of days when daily maximum temperature is below the specified
    percentile for that day, and precipitation is above the specified
    percentile for that day.

    Parameters
    ----------
    tasmax : xarray.DataArray
        Maximum daily temperature.
    pr : xarray.DataArray
        Mean daily precipitation flux.
    tasmax_per : xarray.DataArray
        Percentiles of daily maximum temperature.
    pr_per : xarray.DataArray
        Percentiles of precipitation flux.
    tasmax_per_thresh : float
        Percentile threshold for cool days.
    pr_per_thresh : float
        Percentile threshold for wet days.
    freq : str
        Resampling frequency.
    bootstrap : bool
        Flag to run bootstrapping of percentiles. Used by percentile_bootstrap decorator.
        Bootstrapping is only useful when the percentiles are computed on a part of the studied sample.
        This period, common to percentiles and the sample must be bootstrapped to avoid inhomogeneities with
        the rest of the time series.
        Keep bootstrap to False when there is no common period, it would give wrong results
        plus, bootstrapping is computationally expensive.
    op : {">", ">=", "gt", "ge"}
        Comparison operation. Default: ">".

    Returns
    -------
    xarray.DataArray, [time]
        Count of days with daily maximum temperature below the
        {tasmax_per_thresh}th percentile and precipitation above the
        {pr_per_thresh}th percentile [days].
    """
    comparator = get_op(op=op, constrain=('<', '<='))
    tasmax_per = cast(
        xr.DataArray,
        convert_units_to(source=tasmax_per, target=tasmax),
    ).sel(percentiles=tasmax_per_thresh)
    tasmax_thresh = resample_doy(tasmax_per, tasmax)
    tasmax = comparator(tasmax, tasmax_thresh)

    pr_per = cast(
        xr.DataArray,
        convert_units_to(source=pr_per, target=pr, context='hydro'),
    ).sel(percentiles=pr_per_thresh)
    pr_thresh = resample_doy(doy=pr_per, arr=pr)
    pr_above = comparator(pr, pr_thresh)

    cold_and_wet = xr.ufuncs.logical_and(tasmax, pr_above)
    resampled = cold_and_wet.resample(time=freq).sum(dim='time')
    return to_agg_units(out=resampled, orig=tasmax, op='count')


@declare_units(
    tasmax='[temperature]',
    pr='[precipitation]',
    tasmax_per='[temperature]',
    pr_per='[precipitation]',
)
def cool_and_dry_days(
    tasmax: xr.DataArray,
    pr: xr.DataArray,
    tasmax_per: xr.DataArray,
    pr_per: xr.DataArray,
    tasmax_per_thresh: float = 10,
    pr_per_thresh: float = 90,
    freq: str = 'YS',
    bootstrap: bool = False,  # noqa
    op: str = '<',
) -> xr.DataArray:
    r"""Days with cool and dry conditions.

    Number of days when both daily maximum temperature and
    precipitation are above specified percentiles for that day.

    Parameters
    ----------
    tasmax : xarray.DataArray
        Maximum daily temperature.
    pr : xarray.DataArray
        Mean daily precipitation flux.
    tasmax_per : xarray.DataArray
        Percentiles of daily maximum temperature.
    pr_per : xarray.DataArray
        Percentiles of precipitation flux.
    tasmax_per_thresh : float
        Percentile threshold for cool days.
    pr_per_thresh : float
        Percentile threshold for dry days.
    freq : str
        Resampling frequency.
    bootstrap : bool
        Flag to run bootstrapping of percentiles. Used by percentile_bootstrap decorator.
        Bootstrapping is only useful when the percentiles are computed on a part of the studied sample.
        This period, common to percentiles and the sample must be bootstrapped to avoid inhomogeneities with
        the rest of the time series.
        Keep bootstrap to False when there is no common period, it would give wrong results
        plus, bootstrapping is computationally expensive.
    op : {">", ">=", "gt", "ge"}
        Comparison operation. Default: ">".

    Returns
    -------
    xarray.DataArray, [time]
        Count of days with daily maximum temperature below the
        {tasmax_per_thresh}th percentile and precipitation below the
        {pr_per_thresh}th percentile [days].
    """
    comparator = get_op(op=op, constrain=('<', '<='))

    tasmax_per = cast(
        xr.DataArray,
        convert_units_to(source=tasmax_per, target=tasmax),
    ).sel(percentiles=tasmax_per_thresh)
    tasmax_thresh = resample_doy(tasmax_per, tasmax)
    tasmax = comparator(tasmax, tasmax_thresh)

    pr_per = cast(
        xr.DataArray,
        convert_units_to(source=pr_per, target=pr, context='hydro'),
    ).sel(percentiles=pr_per_thresh)
    pr_thresh = resample_doy(doy=pr_per, arr=pr)
    pr_below = comparator(pr, pr_thresh)

    cold_and_dry = xr.ufuncs.logical_and(tasmax, pr_below)
    resampled = cold_and_dry.resample(time=freq).sum(dim='time')
    return to_agg_units(out=resampled, orig=tasmax, op='count')


# ----------------------------------------------------------------- WARM SPELLS
@declare_units(tasmax='[temperature]', tasmax_per='[temperature]')
def warm_spell_total_days(
    tasmax: xr.DataArray,
    tasmax_per: xr.DataArray,
    per_thresh: float = 90,
    window: int = 3,
    freq: str = 'YS',
    op: Literal['>', '>='] = '>',
    resample_before_rl: bool = True,
) -> xr.DataArray:
    r"""Warm spell total days

    The total number of days that occur within all warm spell events over
    the specified period.
    A warm spell is defined as a period when the daily maximum temperature
    exceeds a specified percentile threshold for at least a minimum number
    of consecutive days.

    Parameters
    ----------
    tasmax : xr.DataArray
        Maximum daily temperature.
    tasmax_per : xr.DataArray
        Percentiles of daily maximum temperature.
    per_thresh : float
        Percentile threshold.
    window : int
        Minimum number of days with temperature above percentile-threshold to
        qualify as a warm spell.
    freq : str
        Resampling frequency.
    op : Literal['>', '>=']
        Comparison operator to use. Constrained to '>' and '>='.
    resample_before_rl : bool
        Determines if the resampling should take place before or after the run
        length encoding (or a similar algorithm) is applied to runs.

    Returns
    -------
    xr.DataArray, [days]
        Total number of days during warm spell events lasting at least
        {window} consecutive days, with daily maximum temperature above the
        {per_thresh}th percentile.
    """
    thresh = tasmax_per.sel(percentiles=per_thresh).pipe(
        convert_units_to, target=tasmax
    )
    assert isinstance(thresh, xr.DataArray)
    thresh = resample_doy(doy=thresh, arr=tasmax)
    above = compare(left=tasmax, op=op, right=thresh, constrain=('>', '>='))
    out = run_length.resample_and_rl(
        da=above,
        resample_before_rl=resample_before_rl,
        compute=run_length.windowed_run_count,
        window=window,
        freq=freq,
    )
    return to_agg_units(out=out, orig=tasmax, op='count')


@declare_units(tasmax='[temperature]', tasmax_per='[temperature]')
def warm_spell_frequency(
    tasmax: xr.DataArray,
    tasmax_per: xr.DataArray,
    per_thresh: float = 90,
    window: int = 3,
    freq: str = 'YS',
    op: Literal['>', '>='] = '>',
    resample_before_rl: bool = True,
) -> xr.DataArray:
    r"""Warm spell frequency.

    The total number of warm spell events during the specified period.
    A warm spell is defined as a period when the daily maximum temperature
    exceeds a specified percentile threshold for at least a minimum number
    of consecutive days.

    Parameters
    ----------
    tasmax : xr.DataArray
        Daily maximum temperature.
    tasmax_per : xr.DataArray
        Percentiles of daily maximum temperature.
    per_thresh : float
        Percentile threshold.
    window : int
        Minimum number of days with temperature above percentile-threshold to
        qualify as a warm spell.
    freq : str
        Resampling frequency.
    op : {'>', '>='}
        Comparison operator to use. Constrained to '>' and '>='.
    resample_before_rl : bool
        Determines if the resampling should take place before or after the run
        length encoding (or a similar algorithm) is applied to runs.

    Returns
    -------
    xr.DataArray
        Total number of warm spell events lasting at least {window}
        consecutive days, with daily maximum temperature above the
        {per_thresh}th percentile.
    """
    thresh = tasmax_per.sel(percentiles=per_thresh).pipe(
        convert_units_to, target=tasmax
    )
    assert isinstance(thresh, xr.DataArray)
    thresh = resample_doy(doy=thresh, arr=tasmax)
    cond = compare(left=tasmax, op=op, right=thresh, constrain=('>', '>='))
    out = run_length.resample_and_rl(
        da=cond,
        resample_before_rl=resample_before_rl,
        compute=run_length.windowed_run_events,
        freq=freq,
        window=window,
    )
    out.attrs['units'] = ''
    return out


@declare_units(tasmax='[temperature]', tasmax_per='[temperature]')
def warm_spell_max_duration(
    tasmax: xr.DataArray,
    tasmax_per: xr.DataArray,
    per_thresh: float = 90,
    window: int = 3,
    freq: str = 'YS',
    op: Literal['>', '>='] = '>',
    resample_before_rl: bool = True,
) -> xr.DataArray:
    r"""Warm spell duration

    The maximum number of days that occur within a single warm spell event
    over the specified period.
    A warm spell is defined as a period when the daily maximum temperature
    exceeds a specified percentile threshold for at least a minimum number
    of consecutive days.

    Parameters
    ----------
    tasmax : xr.DataArray
        Daily maximum temperature.
    tasmax_per : xr.DataArray
        Percentiles of daily maximum temperature.
    per_thresh : float
        Percentile threshold.
    window : int
        Minimum number of days with temperature above percentile-threshold to
        qualify as a warm spell.
    freq : str
        Resampling frequency.
    op : {'>', '>='}
        Comparison operator to use. Constrained to '>' and '>='.
    resample_before_rl : bool
        Determines if the resampling should take place before or after the run
        length encoding (or a similar algorithm) is applied to runs.

    Returns
    -------
    xr.DataArray, [days]
        Duration of the longest warm spell event lasting at least {window}
        consecutive days, with daily maximum temperature above the
        {per_thresh}th percentile.
    """
    thresh = tasmax_per.sel(percentiles=per_thresh).pipe(
        convert_units_to, target=tasmax
    )
    assert isinstance(thresh, xr.DataArray)
    thresh = resample_doy(doy=thresh, arr=tasmax)
    cond = compare(left=tasmax, op=op, right=thresh, constrain=('>', '>='))
    max_length = run_length.resample_and_rl(
        da=cond,
        resample_before_rl=resample_before_rl,
        compute=run_length.longest_run,
        freq=freq,
    )
    out = max_length.where(cond=max_length >= window, other=0)
    return to_agg_units(out=out, orig=tasmax, op='count')


@declare_units(tasmax='[temperature]', tasmax_per='[temperature]')
def warm_spell_max_amplitude(
    tasmax: xr.DataArray,
    tasmax_per: xr.DataArray,
    per_thresh: float = 90,
    window: int = 3,
    freq: str = 'YS',
    resample_before_rl: bool = True,
) -> xr.DataArray:
    r"""Warm spell maximum amplitude

    The maximum cumulative deviation from the percentile threshold during a
    warm spell event. A warm spell is defined as a period when the daily
    maximum temperature exceeds a specified percentile threshold for at least
    a minimum number of consecutive days.

    Parameters
    ----------
    tasmax : xr.DataArray
        Daily maximum temperature.
    tasmax_per : xr.DataArray
        Percentiles of daily maximum temperature.
    per_thresh : float
        Percentile threshold.
    window : int
        Minimum number of days with temperature above percentile-threshold to
        qualify as a warm spell.
    freq : str
        Resampling frequency.
    resample_before_rl : bool
        Determines if the resampling should take place before or after the run
        length encoding (or a similar algorithm) is applied to runs.

    Returns
    -------
    xr.DataArray, [degK]
        Maximum cumulative deviation from the {per_thresh}th percentile
        during a warm spell event lasting at least {window} consecutive
    """
    thresh = tasmax_per.sel(percentiles=per_thresh).pipe(
        convert_units_to, target=tasmax
    )
    assert isinstance(thresh, xr.DataArray)
    thresh = resample_doy(doy=thresh, arr=tasmax)
    above_values = (tasmax - thresh).clip(0)
    out = run_length.resample_and_rl(
        da=above_values,
        resample_before_rl=resample_before_rl,
        compute=run_length.windowed_max_run_sum,
        window=window,
        freq=freq,
    )
    return to_agg_units(out=out, orig=tasmax, op='integral')


# ----------------------------------------------------------------- COLD SPELLS


@declare_units(tasmin='[temperature]', tasmim_per='[temperature]')
def cool_spell_total_days(
    tasmin: xr.DataArray,
    tasmin_per: xr.DataArray,
    per_thresh: float = 10,
    window: int = 3,
    freq: str = 'YS',
    op: Literal['<', '<='] = '<',
    resample_before_rl: bool = True,
) -> xr.DataArray:
    r"""Cool spell total days

    The total number of days that occur within all cool spell events over
    the specified period.
    A cool spell is defined as a period when the daily minimum temperature
    is below a specified percentile threshold for at least a minimum number
    of consecutive days.

    Parameters
    ----------
    tasmin : xr.DataArray
        Daily minimum temperature.
    tasmin_per : xr.DataArray
        Percentiles of daily minimum temperature.
    per_thresh : float
        Percentile threshold.
    window : int
        Minimum number of days with temperature below percentile-threshold to
        qualify as a cool spell.
    freq : str
        Resampling frequency.
    op : Literal['<', '<=']
        Comparison operator to use. Constrained to '<' and '<='.
    resample_before_rl : bool
        Determines if the resampling should take place before or after the run
        length encoding (or a similar algorithm) is applied to runs.

    Returns
    -------
    xr.DataArray, [days]
        Total number of days during cool spell events lasting at least
        {window} consecutive days, with daily minimum temperature below the
        {per_thresh}th percentile.
    """
    thresh = tasmin_per.sel(percentiles=per_thresh).pipe(
        convert_units_to, target=tasmin
    )
    assert isinstance(thresh, xr.DataArray)
    thresh = resample_doy(doy=thresh, arr=tasmin)
    below = compare(left=tasmin, op=op, right=thresh, constrain=('>', '>='))
    out = run_length.resample_and_rl(
        da=below,
        resample_before_rl=resample_before_rl,
        compute=run_length.windowed_run_count,
        window=window,
        freq=freq,
    )
    return to_agg_units(out=out, orig=tasmin, op='count')


@declare_units(tasmin='[temperature]', tasmin_per='[temperature]')
def cool_spell_frequency(
    tasmin: xr.DataArray,
    tasmin_per: xr.DataArray,
    per_thresh: float = 10,
    window: int = 3,
    freq: str = 'YS',
    op: Literal['<', '<='] = '<',
    resample_before_rl: bool = True,
) -> xr.DataArray:
    r"""Cool spell frequency

    The total number of cool spell events during the specified period.
    A cool spell is defined as a period when the daily minimum temperature
    is below a specified percentile threshold for at least a minimum number
    of consecutive days.

    Parameters
    ----------
    tasmin : xr.DataArray
        Daily minimum temperature.
    tasmin_per : xr.DataArray
        Percentiles of daily minimum temperature.
    per_thresh : float
        Percentile threshold.
    window : int
        Minimum number of days with temperature below percentile-threshold to
        qualify as a cool spell.
    freq : str
        Resampling frequency.
    op : {'<', '<='}
        Comparison operator to use. Constrained to '<' and '<='.
    resample_before_rl : bool
        Determines if the resampling should take place before or after the run
        length encoding (or a similar algorithm) is applied to runs.

    Returns
    -------
    xr.DataArray
        Total number of cool spell events lasting at least {window}
        consecutive days, with daily minimum temperature below the
        {per_thresh}th percentile.
    """
    thresh = tasmin_per.sel(percentiles=per_thresh).pipe(
        convert_units_to, target=tasmin
    )
    assert isinstance(thresh, xr.DataArray)
    thresh = resample_doy(doy=thresh, arr=tasmin)
    cond = compare(left=tasmin, op=op, right=thresh, constrain=('>', '>='))
    out = run_length.resample_and_rl(
        da=cond,
        resample_before_rl=resample_before_rl,
        compute=run_length.windowed_run_events,
        freq=freq,
        window=window,
    )
    out.attrs['units'] = ''
    return out


@declare_units(tasmin='[temperature]', tasmin_per='[temperature]')
def cool_spell_max_duration(
    tasmin: xr.DataArray,
    tasmin_per: xr.DataArray,
    per_thresh: float = 10,
    window: int = 3,
    freq: str = 'YS',
    op: Literal['<', '<='] = '<',
    resample_before_rl: bool = True,
) -> xr.DataArray:
    r"""Cool spell duration

    The maximum number of days that occur within a single cool spell event
    over the specified period.
    A cool spell is defined as a period when the daily minimum temperature
    is below a specified percentile threshold for at least a minimum number
    of consecutive days.

    Parameters
    ----------
    tasmin : xr.DataArray
        Daily minimum temperature.
    tasmin_per : xr.DataArray
        Percentiles of daily minimum temperature.
    per_thresh : float
        Percentile threshold.
    window : int
        Minimum number of days with temperature below percentile-threshold to
        qualify as a cool spell.
    freq : str
        Resampling frequency.
    op : {'<', '<='}
        Comparison operator to use. Constrained to '<' and '<='.
    resample_before_rl : bool
        Determines if the resampling should take place before or after the run
        length encoding (or a similar algorithm) is applied to runs.

    Returns
    -------
    xr.DataArray, [days]
        Duration of the longest cool spell event lasting at least {window}
        consecutive days, with daily minimum temperature below the
        {per_thresh}th percentile.
    """
    thresh = tasmin_per.sel(percentiles=per_thresh).pipe(
        convert_units_to, target=tasmin
    )
    assert isinstance(thresh, xr.DataArray)
    thresh = resample_doy(doy=thresh, arr=tasmin)
    cond = compare(left=tasmin, op=op, right=thresh, constrain=('>', '>='))
    max_length = run_length.resample_and_rl(
        da=cond,
        resample_before_rl=resample_before_rl,
        compute=run_length.longest_run,
        freq=freq,
    )
    out = max_length.where(cond=max_length >= window, other=0)
    return to_agg_units(out=out, orig=tasmin, op='count')


@declare_units(tasmax='[temperature]', tasmax_per='[temperature]')
def cool_spell_max_amplitude(
    tasmin: xr.DataArray,
    tasmin_per: xr.DataArray,
    per_thresh: float = 10,
    window: int = 3,
    freq: str = 'YS',
    resample_before_rl: bool = True,
) -> xr.DataArray:
    r"""Cool spell maximum amplitude

    The maximum cumulative deviation from the percentile threshold during a
    cool spell event. A cool spell is defined as a period when the daily
    minimum temperature is below a specified percentile threshold for at least
    a minimum number of consecutive days.

    Parameters
    ----------
    tasmin : xr.DataArray
        Daily minimum temperature.
    tasmin_per : xr.DataArray
        Percentiles of daily minimum temperature.
    per_thresh : float
        Percentile threshold.
    window : int
        Minimum number of days with temperature below percentile-threshold to
        qualify as a cool spell.
    freq : str
        Resampling frequency.
    resample_before_rl : bool
        Determines if the resampling should take place before or after the run
        length encoding (or a similar algorithm) is applied to runs.

    Returns
    -------
    xr.DataArray, [degK]
        Maximum cumulative deviation from the {per_thresh}th percentile
        during a cool spell event lasting at least {window} consecutive days.
    """
    thresh = tasmin_per.sel(percentiles=per_thresh).pipe(
        convert_units_to, target=tasmin
    )
    assert isinstance(thresh, xr.DataArray)
    thresh = resample_doy(doy=thresh, arr=tasmin)
    below_values = (thresh - tasmin).clip(0)
    out = run_length.resample_and_rl(
        da=below_values,
        resample_before_rl=resample_before_rl,
        compute=run_length.windowed_max_run_sum,
        window=window,
        freq=freq,
    )
    return to_agg_units(out=out, orig=tasmin, op='integral')


# def declare_percentile(**percentiles_by_name):
#     def _inner(func: Callable[..., xr.DataArray]):
#         @wraps(func)
#         def wrapper(*args, **kwargs):
#             bound_args = signature(func).bind(*args, **kwargs)
#             bound_args.apply_defaults()
#             for name, per in percentiles_by_name.items():
#                 da = bound_args.arguments[name]
#                 if not isinstance(da, xr.DataArray):
#                     raise ValueError(
#                         f'Argument {name} should be an xarray.DataArray, not {type(da)}'
#                     )
#                 per_value = bound_args.arguments[per]
#                 if not isinstance(per_value, (int, float)):
#                     raise ValueError(
#                         f'Percentile value should be an int or a float, not {type(per_value)}'
#                     )
#                 bound_args.arguments[name] = da.sel(percentiles=per_value)
#
#             return func(*args, **bound_args.arguments)
#
#         return wrapper
#
#     return _inner
