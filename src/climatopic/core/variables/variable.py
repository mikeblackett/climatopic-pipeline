# We hijack the xclim `Indicator` class to benefit from its CF-compliant
# metadata when creating new variables.

from xclim.core.indicator import Indicator


class Variable(Indicator):
    r"""Climate variable base class."""

    __doc__ += Indicator.__doc__  # type: ignore

    realm: str = 'atmos'

    data_flags: dict[str, object] = {}
