"""Utility functions for the `indicators` module."""

from collections.abc import Callable, Generator, Sequence
from os import PathLike
from types import ModuleType
from typing import Literal

from xclim.core.indicator import (
    Indicator,
    build_indicator_module_from_yaml as _build_indicator_module_from_yaml,
)


def build_indicator_module_from_yaml(
    filename: PathLike,
    name: str | None = None,
    indices: dict[str, Callable] | ModuleType | PathLike | None = None,
    translations: dict[str, dict | PathLike] | None = None,
    mode: str = 'raise',
    encoding: str = 'UTF8',
    reload: bool = False,
    validate: bool | PathLike = True,
) -> ModuleType:
    """
    Builds a custom indicator module from a YAML configuration file.

    This is a wrapper around `xclim.indicators.build_indicator_module_from_yaml`
    with an additional function `filter_indicators` for filtering indicators
    based on conditions.

    Parameters
    ----------
    filename : PathLike
        Path to the YAML file containing the indicator configuration.

    name : str or None, optional
        The desired name for the generated Python module. If None, a default name
        is used.

    indices : dict of str to Callable or ModuleType or PathLike or None, optional
        A mapping of indicator names to their corresponding functions, a module
        containing indicator definitions, or a path to a module/directory. If None,
        indicators will be loaded from the YAML file only.

    translations : dict of str to dict or PathLike, optional
        Dictionary of language code to translation mappings or a path to a file
        containing translations. If None, no translations will be applied to the
        indicators.

    mode : str, default='raise'
        Specifies the behavior when encountering errors during module generation.
        Default is 'raise', which raises exceptions on errors.

    encoding : str, default='UTF8'
        Encoding used to read the YAML configuration file. Default is 'UTF8'.

    reload : bool, default=False
        If True, forces reloading of existing modules or configurations.
        Otherwise, caching or preloaded data may be used.

    validate : bool or PathLike, default=True
        Determines if validation of the configuration should be performed. Can
        be a boolean or a path to a validation schema to use.

    Returns
    -------
    ModuleType
        A dynamically generated Python module containing the configured indicators.

    """
    indicators = _build_indicator_module_from_yaml(
        filename=filename,
        name=name,
        indices=indices,
        translations=translations,
        mode=mode,
        encoding=encoding,
        reload=reload,
        validate=validate,
    )

    setattr(
        indicators, 'filter_indicators', filter_indicators_factory(indicators)
    )
    return indicators


def filter_indicators_factory(
    indicators: ModuleType,
) -> Callable[..., Generator[tuple[str, Indicator]]]:
    """
    Create a function to filter indicators based on various criteria.

    Parameters
    ----------
    indicators : ModuleType
        A module containing indicator definitions.

    Returns
    -------
    Callable[..., Generator[tuple[str, Indicator]]]
        A function that filters indicators based on names, keywords, parameters,
        and injected parameters. The function yields tuples of (name, indicator).
        The filtering function accepts the following parameters:
            - names: Sequence[str] | None
                Filter by indicator names
            - keywords: Sequence[str] | None
                Filter by indicator keywords
            - parameters: Sequence[str] | None
                Filter by indicator parameters
            - injected_parameters: Sequence[str] | None
                Filter by indicator injected parameters
            - op: Literal['and', 'or'] = 'and'
                Logical operator to combine multiple filter conditions
    """

    def filter_indicators(
        names: Sequence[str] | None = None,
        keywords: Sequence[str] | None = None,
        parameters: Sequence[str] | None = None,
        injected_parameters: Sequence[str] | None = None,
        op: Literal['and', 'or'] = 'and',
    ) -> Generator[tuple[str, Indicator]]:
        comparator = all if op == 'and' else any
        for name, indicator in indicators.iter_indicators():
            if names is not None and name not in names:
                continue
            if keywords is not None and not comparator(
                keyword in indicator.keywords for keyword in keywords
            ):
                continue
            if parameters is not None and not comparator(
                parameter in indicator.parameters for parameter in parameters
            ):
                continue
            if injected_parameters is not None and not comparator(
                parameter in indicator.injected_parameters
                for parameter in injected_parameters
            ):
                continue
            yield name, indicator

    return filter_indicators
