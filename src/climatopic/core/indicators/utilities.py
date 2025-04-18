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
):
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
