from pathlib import Path

from . import indices
from .indices import PercentileIndex
from .utilities import build_variable_module_from_yaml
from .variable import Variable

__all__ = ['variables', 'PercentileIndex', 'Variable']

variables = build_variable_module_from_yaml(
    filename=Path(__file__).parent / 'variables.yaml',
    indices=indices,
    mode='raise',
    validate=False,
)
