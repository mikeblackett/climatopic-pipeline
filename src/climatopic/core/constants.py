from climatopic.core.types import TChunks

CHUNKS_TIME: TChunks = {
    'time': 'auto',
    'projection_x_coordinate': -1,
    'projection_y_coordinate': -1,
}

CHUNKS_SPACE: TChunks = {
    'time': -1,
    'projection_x_coordinate': 'auto',
    'projection_y_coordinate': 'auto',
}

CHUNKS_PERCENTILE: TChunks = {
    'dayofyear': 'auto',
    # 'percentiles': 1,
    'projection_x_coordinate': -1,
    'projection_y_coordinate': -1,
}

BASELINE_PERIODS: list[tuple[str, str]] = [
    ('1961', '1990'),
    ('1971', '2000'),
    ('1981', '2010'),
    ('1991', '2020'),
]
"""Baseline periods for WMO standard climatological normal periods."""

LOCATIONS: dict[str, str] = {
    'country-united_kingdom': 'United Kingdom',
    'country-england_and_wales': 'England and Wales',
    'country': 'UK countries',
    'region': 'UK regions',
    'river': 'UK river basin districts',
}
"""Geographic locations for UKCP18 data."""
