import numpy as np

time = {
    'name': 'time',
    'dims': ('time',),
    'dtype': np.datetime64,
    'attrs': {
        'attrs': {
            'axis': {'type': str, 'value': 'T'},
            'standard_name': {'type': str, 'value': 'time'},
            'bounds': {'type': str, 'value': 'time_bounds'},
        },
        'require_all_keys': True,
        'allow_extra_keys': True,
    },
}

dayofyear = {
    'name': 'dayofyear',
    'dims': ('dayofyear',),
    'dtype': np.int64,
}

percentiles = {
    'name': 'percentiles',
    'dims': ('percentiles',),
    'dtype': np.int64,
}

projection_x_coordinate = {
    'name': 'projection_x_coordinate',
    'dims': ('projection_x_coordinate',),
    'dtype': np.float64,
    'attrs': {
        'attrs': {
            'axis': {'type': str, 'value': 'X'},
            'units': {'type': str, 'value': 'm'},
            'standard_name': {'type': str, 'value': 'projection_x_coordinate'},
            'bounds': {'type': str, 'value': 'projection_x_coordinate_bounds'},
        },
        'require_all_keys': True,
        'allow_extra_keys': True,
    },
}

projection_y_coordinate = {
    'name': 'projection_y_coordinate',
    'dims': ('projection_y_coordinate',),
    'dtype': np.float64,
    'attrs': {
        'attrs': {
            'axis': {'type': str, 'value': 'Y'},
            'units': {'type': str, 'value': 'm'},
            'standard_name': {'type': str, 'value': 'projection_y_coordinate'},
            # 'bounds': {'type': str, 'value': 'projection_y_coordinate_bounds'},
        },
        'require_all_keys': True,
        'allow_extra_keys': True,
    },
}

time_bounds = {
    'name': 'time_bounds',
    'dims': ('time', 'bounds'),
    'coords': {'coords': {'time': time}},
    'dtype': np.datetime64,
    'attrs': {
        'attrs': {},
        'require_all_keys': False,
        'allow_extra_keys': False,
    },
}

projection_x_coordinate_bounds = {
    'name': 'projection_x_coordinate_bounds',
    'dims': ('projection_x_coordinate', 'bounds'),
    'coords': {'coords': {'projection_x_coordinate': projection_x_coordinate}},
    'dtype': np.float64,
    'attrs': {
        'attrs': {},
        'require_all_keys': False,
        'allow_extra_keys': False,
    },
}

projection_y_coordinate_bounds = {
    'name': 'projection_y_coordinate_bounds',
    'dims': ('projection_y_coordinate', 'bounds'),
    'coords': {'coords': {'projection_y_coordinate': projection_y_coordinate}},
    'dtype': np.float64,
    'attrs': {
        'attrs': {},
        'require_all_keys': False,
        'allow_extra_keys': False,
    },
}

spatial_coordinates = {
    'projection_x_coordinate': projection_x_coordinate,
    'projection_y_coordinate': projection_y_coordinate,
}

spatial_bounds_coordinates = {
    'projection_x_coordinate_bounds': projection_x_coordinate_bounds,
    'projection_y_coordinate_bounds': projection_y_coordinate_bounds,
}

temporal_coordinates = {
    'time': time,
}

temporal_bounds_coordinates = {
    'time_bounds': time_bounds,
}

temporal_coordinates_with_bounds = (
    temporal_coordinates | temporal_bounds_coordinates
)

spatial_coordinates_with_bounds = (
    spatial_coordinates | spatial_bounds_coordinates
)

spatio_temporal_coordinates = temporal_coordinates | spatial_coordinates
spatio_temporal_coordinates_with_bounds = (
    temporal_coordinates_with_bounds | spatial_coordinates_with_bounds
)
