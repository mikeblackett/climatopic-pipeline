__author__ = """Mike Blackett"""
__email__ = 'mike.blackett13@gmail.com'
__version__ = 'unknown'

try:
    from _version import __version__  # noqa: F401
except ImportError:  #
    pass

import cf_xarray  # noqa: F401
import climatopic_xarray.bounds  # noqa: F401

from .definitions import defs  # noqa: F401
