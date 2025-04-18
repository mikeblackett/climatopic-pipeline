from pathlib import Path

from . import indices
from .utilities import (
    build_indicator_module_from_yaml,
)

__all__ = ['indicators']

indicators = build_indicator_module_from_yaml(
    filename=Path(__file__).parent / 'indicators.yaml',
    indices=indices,
    mode='raise',
)
