import datetime as dt
from collections.abc import Hashable, Mapping, Sequence
from typing import Literal

import pandas as pd
from dagster import (
    AssetDep,
    AssetIn,
    AssetKey,
    AssetOut,
    AssetSpec,
    AssetsDefinition,
    PartitionMapping,
    PartitionsDefinition,
)
from xarray.core.types import T_ChunkDim

type TDatetimeLike = str | dt.date | dt.datetime | pd.Timestamp

type TChunkDim = T_ChunkDim
# Like xarray T_Chunks, but named dims only
type TChunks = Mapping[str | Hashable, T_ChunkDim] | None

# Assets
type TCoercibleToAssetKey = AssetKey | str | Sequence[str]
type TCoercibleToAssetKeyPrefix = str | Sequence[str]
type TCoercibleToAssetDep = (
    TCoercibleToAssetKey | AssetSpec | AssetsDefinition | AssetDep
)
type TCoercibleToAssetIn = (
    TCoercibleToAssetKey | AssetSpec | AssetsDefinition | AssetIn
)
type CoercibleToAssetOut = (
    TCoercibleToAssetKey | AssetSpec | AssetsDefinition | AssetOut
)

# Partitions
type TPartitionKeys = Sequence[str] | Mapping[str, Sequence[str] | None]
type TCoercibleToPartitionsDefinition = TPartitionKeys | PartitionsDefinition
type TCoercibleToPartitionMapping = TPartitionKeys | PartitionMapping

type FrequencyName = Literal['day', 'month', 'season', 'year']
"""Type alias for frequencies."""
