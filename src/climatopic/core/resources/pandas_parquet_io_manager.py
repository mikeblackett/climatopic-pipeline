from pathlib import Path

import geopandas as gpd
import pandas as pd
from dagster import InputContext, OutputContext, UPathIOManager


class PandasParquetIOManager(UPathIOManager):
    """An IOManager for reading and writing Pandas DataFrame from Parquet files."""

    base_path: str | None = None
    """The base path to use when reading or writing data frames."""

    extension: str | None = '.parquet'
    """The extension to use when writing data frames. Defaults to '.parquet'."""

    def dump_to_path(
        self,
        context: OutputContext,
        obj: pd.DataFrame | gpd.GeoDataFrame,
        path: Path,
    ) -> None:
        with path.open('wb') as file:
            obj.to_parquet(path=file)

    def load_from_path(
        self, context: InputContext, path: Path
    ) -> pd.DataFrame:
        with path.open('rb') as file:
            return pd.read_parquet(file)


class GeoPandasParquetIOManager(PandasParquetIOManager):
    """An IOManager for reading and writing Geopandas DataFrame from Parquet files."""

    def load_from_path(
        self, context: InputContext, path: Path
    ) -> gpd.GeoDataFrame:
        with path.open('rb') as file:
            return gpd.read_parquet(file)
