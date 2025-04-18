from typing import Any

import xarray as xr
from dagster import InputContext, OutputContext, UPathIOManager
from upath import UPath

from climatopic.core.utilities import open_mf_dataset


class HadukIOManager(UPathIOManager):
    """An IOManager for reading multi-file HadUK-Grid netcdf datasets to xarray."""

    release: str
    version: str
    resolution: str

    def __init__(
        self,
        base_path: UPath,
        release: str,
        version: str,
        resolution: str,
    ) -> None:
        super().__init__(base_path)
        self.release: str = release
        self.version: str = version
        self.resolution: str = resolution

    def get_path_for_partition(
        self,
        context: InputContext | OutputContext,
        path: UPath,
        partition: str,
    ) -> UPath:
        # TODO: This is a placeholder implementation
        return (
            self._base_path
            / self.version
            / self.resolution
            / path.stem
            / partition
            / self.release
        )

    def load_from_path(
        self,
        context: InputContext,
        path: UPath,
    ) -> xr.Dataset:
        return open_mf_dataset(path=path, decode_coords='all', sort=True)

    def dump_to_path(
        self, context: OutputContext, obj: Any, path: UPath
    ) -> None:
        raise NotImplementedError
