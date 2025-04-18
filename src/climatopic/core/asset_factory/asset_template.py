from collections.abc import Iterable, Mapping, Sequence, Set
from functools import cached_property
from typing import Any

from dagster import (
    AssetDep,
    AssetIn,
    AssetKey,
    AssetOut,
    AssetSpec,
    AssetsDefinition,
    DagsterInvariantViolationError,
    DagsterType,
    PartitionMapping,
    PartitionsDefinition,
)

from climatopic.core.types import (
    TCoercibleToAssetDep,
    TCoercibleToAssetIn,
    TCoercibleToAssetKey,
)

EMPTY_ASSET_KEY_SENTINEL = AssetKey([])


class AssetTemplate:
    """
    Defines an asset template for creating and transforming assets.

    An asset template is used to define a general asset specification that can
    be reused throughout the DAG. You can pass around `AssetTemplates` to
    define implicit dependencies across modules. Importing templates instead of
    fully-fledged Dagster asset definitions means that you can import them and
    still use Dagster utilities like `Dagster.load_assets_from_modules()`
    without incurring duplicate asset errors.

    Attributes
    ----------
    key: AssetKey
        The unique identifier for this asset
    code_version: str | None
        The version of the code for this specific asset.
    dagster_type: DagsterType | None
        Allows specifying type validation functions that will be executed on
        the input of the decorated function before it runs.
    deps: Iterable[AssetDep]
        The asset keys for the upstream assets that this asset depends on.
    description: str | None
        Human-readable description of this asset.
    group_name: str | None
        A string name used to organize multiple assets into groups. If not
        provided, the name "default" is used.
    ins: Mapping[str, AssetIn]
        A dictionary that maps input names to information about the input.
    io_manager_key: str | None
        The resource key of the IOManager used for storing the output of the
        op as an asset, and for loading it in downstream ops. If not provided,
        the default IOManager is used.
    key_prefix: Sequence[str]
        The prefix of the asset key. This is all elements of the asset key
        except the last one.
    kinds: Set[str]
        A list of strings representing the kinds of the asset. These will be
        made visible in the Dagster UI.
    metadata: Mapping[str, Any]
        A dictionary of metadata entries for the asset.
    name: str
        The name of the asset. This is the last element of the asset key.
    owners: Sequence[str] | None
        A list of strings representing owners of the asset.
    partitions_def: PartitionsDefinition | None
        Defines the set of partition keys that compose the asset.
    skippable: bool
        Whether this asset can be omitted during materialization, causing
        downstream dependencies to skip.
    tags: Mapping[str, str]
        A dictionary of tags for filtering and organizing. These tags are not
        attached to runs of the asset.
    """

    def __init__(
        self,
        key: TCoercibleToAssetKey,
        *,
        code_version: str | None = None,
        dagster_type: DagsterType | None = None,
        deps: Iterable[TCoercibleToAssetDep] | None = None,
        description: str | None = None,
        group_name: str | None = None,
        ins: Mapping[str, TCoercibleToAssetIn] | None = None,
        io_manager_key: str | None = None,
        kinds: Set[str] | None = None,
        metadata: Mapping[str, Any] | None = None,
        owners: Sequence[str] | None = None,
        partitions_def: PartitionsDefinition[str] | None = None,
        skippable: bool = False,
        tags: Mapping[str, str] | None = None,
    ):
        """
        Builds an AssetTemplate.

        Parameters
        ----------
        key: AssetKey
            The unique identifier for this asset.
        code_version: str | None
            The version of the code for this specific asset.
        dagster_type: DagsterType | None
            Allows specifying type validation functions that will be executed on
            the input of the decorated function before it runs.
        deps: Iterable[AssetDep]
            The asset keys for the upstream assets that this asset depends on.
        description: str | None
            Human-readable description of this asset.
        group_name: str | None
            A string name used to organize multiple assets into groups. If not
            provided, the name "default" is used.
        ins: Mapping[str, AssetIn]
            A dictionary that maps input names to information about the input.
        io_manager_key: str | None
            The resource key of the IOManager used for storing the output of the
            op as an asset, and for loading it in downstream ops. If not provided,
            the default IOManager is used.
        kinds: Set[str]
            A list of strings representing the kinds of the asset. These will be
            made visible in the Dagster UI.
        metadata: Mapping[str, Any]
            A dictionary of metadata entries for the asset.
        owners: Sequence[str] | None
            A list of strings representing owners of the asset.
        partitions_def: PartitionsDefinition | None
            Defines the set of partition keys that compose the asset.
        skippable: bool
            Whether this asset can be omitted during materialization, causing
            downstream dependencies to skip.
        tags: Mapping[str, str]
            A dictionary of tags for filtering and organizing. These tags are
            not attached to runs of the asset.

        Returns
        -------
        AssetTemplate
            The asset template built from the parameters.
        """

        self._spec = AssetSpec(
            code_version=code_version,
            deps=deps,
            description=description,
            group_name=group_name,
            key=AssetKey.from_coercible(key),
            kinds=set(kinds or []),
            metadata=metadata,
            owners=owners,
            partitions_def=partitions_def,
            skippable=skippable,
            tags=tags,
        )
        self._ins = _coerce_to_ins(ins)
        self.dagster_type = dagster_type
        self.io_manager_key = io_manager_key

    @property
    def code_version(self) -> str | None:
        """The version of the code for this specific asset."""
        return self._spec.code_version

    @property
    def deps(self) -> Iterable[AssetDep]:
        """The asset keys for the upstream assets that this asset depends on."""
        return self._spec.deps

    @property
    def description(self) -> str | None:
        """Human-readable description of this asset."""
        return self._spec.description

    @property
    def group_name(self) -> str | None:
        """A string name used to organize multiple assets into groups."""
        return self._spec.group_name

    @cached_property
    def ins(self) -> Mapping[str, AssetIn]:
        """A dictionary that maps input names to information about the input."""
        return self._ins

    @property
    def key(self) -> AssetKey:
        """The unique identifier for this asset."""
        return self._spec.key

    @property
    def key_prefix(self) -> Sequence[str]:
        """The prefix of the asset key."""
        return self._spec.key.path[:-1]

    @property
    def kinds(self) -> Set[str]:
        """A list of strings representing the kinds of the asset."""
        return self._spec.kinds

    @property
    def metadata(self) -> Mapping[str, Any]:
        """A dictionary of metadata entries for the asset."""
        return self._spec.metadata

    @property
    def name(self) -> str:
        """The name of the asset."""
        return self._spec.key.path[-1]

    @property
    def owners(self) -> Sequence[str]:
        """A list of strings representing owners of the asset."""
        return self._spec.owners

    @property
    def partitions_def(self) -> PartitionsDefinition | None:
        """Defines the set of partition keys that compose the asset."""
        return self._spec.partitions_def

    @property
    def skippable(self) -> bool:
        """Whether this asset can be omitted during materialization."""
        return self._spec.skippable

    @property
    def spec(self) -> AssetSpec:
        """The asset specification for this asset template."""
        return self._spec

    @property
    def tags(self) -> Mapping[str, str]:
        """A dictionary of tags for filtering and organizing."""
        return self._spec.tags

    def to_spec(
        self,
        key: AssetKey | None = None,
        deps: Sequence[AssetDep] | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> AssetSpec:
        """
        Builds an AssetSpec from the template.

        You can pass optional parameters to augment/override the template's
        values.

        Parameters
        ----------
        key: AssetKey | None
            The key of the asset. This will override the template's key.
        deps: Sequence[AssetDep] | None
            The dependencies of the asset. This will augment the template's
            dependencies.
        tags: Mapping[str, str] | None
            The tags of the asset. This will augment the template's tags.
            If a tag is present in both the template and the passed tags,
            the passed tag will override the template's tag.

        Returns
        -------
        AssetSpec
            The asset specification built from the template.
        """
        return self._spec.replace_attributes(
            key=key or self.key,
            deps=[*self.deps, *(deps or [])],
            tags={**self.tags, **(tags or {})},
        )

    def to_out(
        self,
        dagster_type: DagsterType | None = None,
        io_manager_key: str | None = None,
        is_required: bool | None = None,
    ) -> AssetOut:
        """
        Builds an AssetOut from the template.

        You can pass optional parameters to augment/override the template's
        values.

        Parameters
        ----------
        dagster_type: DagsterType | None
            The Dagster type of the asset. This will override the template's
            dagster type.
        io_manager_key: str | None
            The IO manager key for the asset. This will override the template's
            IO manager key.
        is_required: bool | None
            Whether the asset is required. This will override the template's
            skippable attribute.

        Returns
        -------
        AssetOut
            The asset output built from the template.
        """
        return AssetOut.from_spec(
            # Can't create AssetOut from AssetSpec with deps
            spec=self._spec.replace_attributes(deps=[]),
            dagster_type=dagster_type or self.dagster_type,  # type: ignore
            io_manager_key=io_manager_key or self.io_manager_key,
            is_required=is_required or self._spec.skippable,
        )

    def to_in(
        self,
        key_prefix: Sequence[str] | None = None,
        metadata: Mapping[str, Any] | None = None,
        partition_mapping: PartitionMapping | None = None,
    ) -> AssetIn:
        """
        Builds an AssetIn from the template.

        Parameters
        ----------
        key_prefix: Sequence[str] | None
            The prefix of the asset key. The key will be the concatenation of
            the key prefix and the template's key.
        metadata: Mapping[str, Any] | None
            A dictionary of metadata entries for the asset in.
        partition_mapping: PartitionMapping | None
            Defines what partitions to depend on in the upstream asset. If not
            provided, defaults to the default partition mapping for the
            partitions definition.

        Returns
        -------
        AssetIn
            The asset input built from the template.
        """
        return AssetIn(
            key=self.key.with_prefix(key_prefix or []),
            metadata=metadata,
            partition_mapping=partition_mapping,
            dagster_type=self.dagster_type,  # type: ignore
        )

    def replace_attributes(
        self,
        *,
        code_version: str | None = None,
        dagster_type: DagsterType | None = None,
        deps: Iterable[TCoercibleToAssetDep] | None = None,
        description: str | None = None,
        group_name: str | None = None,
        ins: Mapping[str, TCoercibleToAssetIn] | None = None,
        io_manager_key: str | None = None,
        key: TCoercibleToAssetKey | None = None,
        kinds: Set[str] | None = None,
        metadata: Mapping[str, Any] | None = None,
        owners: Sequence[str] | None = None,
        partitions_def: PartitionsDefinition[str] | None = None,
        skippable: bool = False,
        tags: Mapping[str, str] | None = None,
    ):
        """Returns a new AssetTemplate with the specified attributes replaced."""
        return AssetTemplate(
            code_version=code_version or self.code_version,
            dagster_type=dagster_type or self.dagster_type,
            deps=deps or self.deps or [],
            description=description or self.description,
            group_name=group_name or self.group_name,
            ins=ins or self._ins,
            io_manager_key=io_manager_key or self.io_manager_key,
            key=key or self.key,
            kinds=kinds or self.kinds,
            metadata=metadata or self.metadata or {},
            owners=owners or self.owners,
            partitions_def=partitions_def or self.partitions_def,
            skippable=skippable or self.skippable,
            tags=tags or self.tags,
        )

    def merge_attributes(
        self,
        *,
        deps: Iterable[TCoercibleToAssetDep] | None = None,
        ins: Mapping[str, TCoercibleToAssetIn] | None = None,
        kinds: set[str] | None = None,
        metadata: Mapping[str, Any] | None = None,
        tags: Mapping[str, str] | None = None,
    ):
        """Returns a new AssetTemplate with the specified attributes merged."""
        return AssetTemplate(
            code_version=self.code_version,
            dagster_type=self.dagster_type,
            deps=[*(self.deps or []), *(deps or [])],
            description=self.description,
            group_name=self.group_name,
            ins={**self.ins, **(ins or {})},
            io_manager_key=self.io_manager_key,
            key=self.key,
            kinds={*(self.kinds or []), *(kinds or [])},
            metadata={**(self.metadata or {}), **(metadata or {})},
            owners=self.owners,
            partitions_def=self.partitions_def,
            skippable=self.skippable,
            tags={**(self.tags or {}), **(tags or {})},
        )

    @staticmethod
    def from_spec(
        spec: AssetSpec,
        *,
        dagster_type: DagsterType | None = None,
        ins: Mapping[str, TCoercibleToAssetIn] | None = None,
        io_manager_key: str | None = None,
    ):
        """
        Builds an AssetTemplate from an AssetSpec.

        Parameters
        ----------
        spec: AssetSpec
            The asset specification to build the template from.
        dagster_type: DagsterType | None
            The Dagster type of the asset.
        ins: Mapping[str, TCoercibleToAssetIn] | None
            A dictionary that maps input names to information about the input.
        io_manager_key: str | None
            The IO manager key for the asset.

        Returns
        -------
        AssetTemplate
            The asset template built from the specification.
        """
        return AssetTemplate(
            code_version=spec.code_version,
            dagster_type=dagster_type,
            deps=spec.deps,
            description=spec.description,
            group_name=spec.group_name,
            ins=ins,
            io_manager_key=io_manager_key,
            key=spec.key,
            kinds=spec.kinds,
            metadata=spec.metadata,
            owners=spec.owners,
            partitions_def=spec.partitions_def,
            skippable=spec.skippable,
            tags=spec.tags,
        )


class AssetGroup:
    """
    Defines a group of asset templates that share a common specification and
    materialization strategy.

    The properties defined on the asset group are shared by the asset
    templates in the group. Any existing template properties are merged with
    the group properties.

    Asset groups are not typically created directly but are instead created
    behind the scenes when using the `@asset_factory` decorator.

    Attributes
    ----------
    code_version: str | None
        The version of the code for this asset group.
    dagster_type: DagsterType | None
        Allows specifying type validation functions that will be executed on
        the input of the decorated function before it runs.
    deps: Iterable[AssetDep]
        The asset keys for the upstream assets that this asset depends on.
    description: str | None
        Human-readable description of this asset.
    group_name: str | None
        A string name used to organize multiple assets into groups. If not
        provided, the name "default" is used.
    ins: Mapping[str, AssetIn]
        A dictionary that maps input names to information about the input.
    io_manager_key: str | None
        The resource key of the IOManager used for storing the output of the
        op as an asset, and for loading it in downstream ops. If not provided,
        the default IOManager is used.
    key_prefix: Sequence[str]
        The prefix of the asset key. This is all elements of the asset key
        except the last one.
    kinds: Set[str]
        A list of strings representing the kinds of the asset. These will be
        made visible in the Dagster UI.
    metadata: Mapping[str, Any]
        A dictionary of metadata entries for the asset.
    name: str
        The name of the asset. This is the last element of the asset key.
    owners: Sequence[str] | None
        A list of strings representing owners of the asset.
    partitions_def: PartitionsDefinition | None
        Defines the set of partition keys that compose the asset.
    skippable: bool
        Whether this asset can be omitted during materialization, causing
        downstream dependencies to skip.
    tags: Mapping[str, str]
        A dictionary of tags for filtering and organizing. These tags are not
        attached to runs of the asset.
    """

    def __init__(
        self,
        name: str,
        *,
        code_version: str | None = None,
        dagster_type: DagsterType | None = None,
        deps: Iterable[AssetDep] | None = None,
        description: str | None = None,
        group_name: str | None = None,
        ins: Mapping[str, TCoercibleToAssetIn] | None = None,
        io_manager_key: str | None = None,
        key_prefix: str | Sequence[str] | None = None,
        kinds: Iterable[str] | None = None,
        metadata: Mapping[str, Any] | None = None,
        partitions_def: PartitionsDefinition[str] | None = None,
        templates: Iterable['AssetTemplate'] | None = None,
        tags: Mapping[str, str] | None = None,
    ):
        self.name = name
        self.key_prefix = (
            [key_prefix] if isinstance(key_prefix, str) else key_prefix or []
        )
        self._template = AssetTemplate(
            code_version=code_version,
            dagster_type=dagster_type,
            deps=deps,
            description=description,
            group_name=group_name,
            ins=ins,
            io_manager_key=io_manager_key,
            key=EMPTY_ASSET_KEY_SENTINEL,
            kinds=set(kinds or []),
            metadata=metadata,
            partitions_def=partitions_def,
            skippable=True,
            tags=tags,
        )
        self._templates = [
            template.replace_attributes(
                key=template.key.with_prefix(self.key_prefix),
                partitions_def=self.partitions_def,
                code_version=self.code_version or template.code_version,
            ).merge_attributes(
                deps=self.deps,
                ins=self.ins,
                metadata=self.metadata,
                tags=self.tags,
                kinds=set(self.kinds),
            )
            for template in templates or []
        ]

    def __getattr__(self, name: str) -> Any:
        try:
            # Delegate attribute access to the template
            return getattr(self._template, name)
        except AttributeError as error:
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'"
            ) from error

    @property
    def templates(self) -> Sequence['AssetTemplate']:
        """The asset templates in this group."""
        return self._templates

    @cached_property
    def specs(self) -> Sequence[AssetSpec]:
        """The asset specifications for the assets in this group."""
        return [template.spec for template in self.templates]

    def to_assets_def(self):
        """Builds an AssetsDefinition from the asset group."""
        specs = (
            [
                template.spec.with_io_manager_key(self.io_manager_key)
                for template in self.templates
            ]
            if self.io_manager_key
            else self.specs
        )
        return AssetsDefinition(specs=specs)


def _coerce_to_ins(
    coercible_to_asset_ins: Mapping[str, TCoercibleToAssetIn] | None = None,
) -> Mapping[str, AssetIn]:
    """Coerces a mapping of input names to AssetIn objects."""
    if not coercible_to_asset_ins:
        return {}
    ins_set = {}
    for input_name, in_ in coercible_to_asset_ins.items():
        asset_in = _asset_in_from_coercible(in_)
        if input_name in ins_set and asset_in != ins_set[asset_in.key]:
            raise DagsterInvariantViolationError(
                f'Cannot set a dependency on asset {asset_in.key} '
                'more than once per asset.'
            )
        ins_set[input_name] = asset_in

    return ins_set


def _asset_in_from_coercible(arg: TCoercibleToAssetIn) -> AssetIn:
    """Coerces a value to an AssetIn object."""
    match arg:
        case AssetIn():
            return arg
        case AssetKey():
            return AssetIn(key=arg)
        case str() | list():
            return AssetIn(key=AssetKey.from_coercible(arg))
        case AssetSpec():
            return AssetIn(
                key=arg.key,
                metadata=arg.metadata,
            )
        case _:
            raise TypeError(f'Invalid type for asset in: {arg}')
