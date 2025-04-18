from collections.abc import Callable, Iterable, Mapping
from functools import wraps

from dagster import (
    AssetDep,
    AssetExecutionContext,
    AssetsDefinition,
    DagsterType,
    Output,
    PartitionsDefinition,
    asset,
)

from climatopic.core.types import (
    TCoercibleToAssetIn,
    TCoercibleToAssetKeyPrefix,
)
from .asset_template import AssetGroup, AssetTemplate


def asset_factory[T](
    name: str,
    *,
    code_version: str | None = None,
    dagster_type: DagsterType | None = None,
    deps: Iterable[AssetDep] | None = None,
    description: str | None = None,
    group_name: str | None = None,
    ins: Mapping[str, TCoercibleToAssetIn] | None = None,
    io_manager_key: str | None = None,
    key_prefix: TCoercibleToAssetKeyPrefix | None = None,
    kinds: Iterable[str] | None = None,
    metadata: Mapping[str, object] | None = None,
    partitions_def: PartitionsDefinition[str] | None = None,
    templates: Iterable[AssetTemplate] | None = None,
    tags: Mapping[str, str] | None = None,
) -> Callable[..., list[AssetsDefinition]]:
    """
    Creates a definition for how to compute a set of assets that share a
    common specification and materialization strategy.

    Parameters
    ----------
    name: str
        The name of the asset group.
    code_version: str | None
        The code version of the asset group.
    dagster_type: DagsterType | None
        The Dagster type of the asset group.
    deps: Iterable[AssetDep] | None
        The dependencies of the asset group.
    description: str | None
        The description of the asset group.
    group_name: str | None
        The group name of the asset group.
    ins: Mapping[str, TCoercibleToAssetIn] | None
        The inputs of the asset group.
    io_manager_key: str | None
        The IO manager key of the asset group.
    key_prefix: TCoercibleToAssetKeyPrefix | None
        The key prefix of the asset group.
    kinds: Iterable[str] | None
        The kinds of the asset group.
    metadata: Mapping[str, object] | None
        The metadata of the asset group.
    partitions_def: PartitionsDefinition[str] | None
        The partitions definition of the asset group.
    templates: Iterable[AssetTemplate] | None
        The templates of the asset group.
    tags: Mapping[str, str] | None
        The tags of the asset group.

    Returns
    -------
    Callable[..., list[AssetsDefinition]]
        A decorator that can be used to decorate the compute function for the
        assets.

    Raises
    ------
    ValueError
        If no templates are provided.
    """
    asset_group = AssetGroup(
        code_version=code_version,
        dagster_type=dagster_type,
        deps=deps,
        description=description,
        group_name=group_name,
        ins=ins,
        io_manager_key=io_manager_key,
        key_prefix=key_prefix,
        kinds=kinds,
        metadata=metadata,
        name=name,
        partitions_def=partitions_def,
        templates=templates,
        tags=tags,
    )
    if not templates:
        raise ValueError(
            'No templates provided. Please provide at least one template.'
        )

    asset_defs: list[AssetsDefinition] = []

    def wrapper(compute_fn: Callable[..., Output[T]]) -> list[AssetsDefinition]:
        for template in asset_group.templates:

            @asset(
                code_version=code_version,
                dagster_type=asset_group.dagster_type,
                description=template.description,
                group_name=template.group_name,
                ins=template.ins,
                io_manager_key=asset_group.io_manager_key,
                key=template.key,
                kinds=template.kinds,
                metadata=template.metadata,
                partitions_def=template.partitions_def,
                tags=template.tags,
            )
            @wraps(compute_fn)
            def _asset(
                context: AssetExecutionContext,
                *args,
                **kwargs,
            ) -> Output[T]:
                return compute_fn(context, *args, **kwargs)

            asset_defs.append(_asset)

        return asset_defs

    return wrapper
