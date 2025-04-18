from __future__ import annotations

from ._percentile import (
    PercentileIndice,
    tas_per,
    tasmax_per,
    tasmin_per,
    pr_per,
)
from ._simple import (
    pr,
    tas,
    tasmax,
    tasmin,
    rainfall,
)

__all__ = [
    'PercentileIndice',
    'pr',
    'pr_per',
    'rainfall',
    'tas',
    'tas_per',
    'tasmax',
    'tasmax_per',
    'tasmin',
    'tasmin_per',
]
