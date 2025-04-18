from typing import Any

from . import coords

haduk_dataset_schema: dict[str, Any] = {
    'coords': {
        'coords': coords.spatio_temporal_coordinates,
        'require_all_keys': True,
        'allow_extra_keys': True,
    },
    'dims': tuple(coords.spatio_temporal_coordinates.keys()),
}

doy_dataset_schema: dict[str, Any] = {
    'coords': {
        'coords': {
            'dayofyear': coords.dayofyear,
            **coords.spatial_coordinates,
            'percentiles': coords.percentiles,
        },
        'require_all_keys': True,
        'allow_extra_keys': True,
    },
    'dims': (
        'dayofyear',
        *tuple(coords.spatial_coordinates.keys()),
        'percentiles',
    ),
}

climatopic_dataset_schema: dict[str, Any] = {
    'coords': {
        'coords': coords.spatio_temporal_coordinates,
        'require_all_keys': True,
        'allow_extra_keys': True,
    },
    'dims': tuple(coords.spatio_temporal_coordinates.keys()),
}
