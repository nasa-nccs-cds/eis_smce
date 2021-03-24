# -*- coding: utf-8 -*-
import fsspec
from distutils.version import LooseVersion
from intake.source.base import PatternMixin
from intake.source.utils import reverse_format
from intake_xarray.base import DataSourceMixin


class ModisPointSource(DataSourceMixin, PatternMixin):

    name = 'modis_points'

    def __init__(self, urlpath, chunks=None, combine=None, concat_dim=None,
                 xarray_kwargs=None, metadata=None,
                 path_as_pattern=True, storage_options=None, **kwargs):
        self.path_as_pattern = path_as_pattern
        self.urlpath = urlpath
        self.chunks = chunks
        self.concat_dim = concat_dim
        self.combine = combine
        self.storage_options = storage_options or {}
        self.xarray_kwargs = xarray_kwargs or {}
        self._ds = None
        if isinstance(self.urlpath, list):
            self._can_be_local = fsspec.utils.can_be_local(self.urlpath[0])
        else:
            self._can_be_local = fsspec.utils.can_be_local(self.urlpath)
        super(ModisPointSource, self).__init__(metadata=metadata, **kwargs)

    def _open_dataset(self):
        import xarray as xr
        url = self.urlpath

        kwargs = self.xarray_kwargs

        if "*" in url or isinstance(url, list):
            _open_dataset = xr.open_mfdataset
            if self.pattern:
                kwargs.update(preprocess=self._add_path_to_ds)
            if self.combine is not None:
                if 'combine' in kwargs:
                    raise Exception("Setting 'combine' argument twice  in the catalog is invalid")
                kwargs.update(combine=self.combine)
            if self.concat_dim is not None:
                if 'concat_dim' in kwargs:
                    raise Exception("Setting 'concat_dim' argument twice  in the catalog is invalid")
                kwargs.update(concat_dim=self.concat_dim)
        else:
            _open_dataset = xr.open_dataset

        if self._can_be_local:
            url = fsspec.open_local(self.urlpath, **self.storage_options)
        else:
            # https://github.com/intake/filesystem_spec/issues/476#issuecomment-732372918
            url = fsspec.open(self.urlpath, **self.storage_options).open()

        self._ds = _open_dataset(url, chunks=self.chunks, **kwargs)

    def _add_path_to_ds(self, ds):
        """Adding path info to a coord for a particular file
        """
        var = next(var for var in ds)
        new_coords = reverse_format(self.pattern, ds[var].encoding['source'])
        return ds.assign_coords(**new_coords)
