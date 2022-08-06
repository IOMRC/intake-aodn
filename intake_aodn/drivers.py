#!/usr/bin/env python
#-----------------------------------------------------------------------------
# Copyright (c) 2020 - 2021, CSIRO 
#
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------
from intake_xarray.base import DataSourceMixin
import pandas as pd
import xarray as xr
from dask import delayed, compute
from fsspec.core import url_to_fs
import shapely.wkt
import pandas as pd
import os
import logging
import warnings
import json
import fsspec
logger = logging.getLogger('intake-aodn')

class RefZarrStackSource(DataSourceMixin):
    """An extension of intake-xarray in an opinionated fashion to open a stack of AODN data stored as zarr references using fsspec.
    Expands urlpath with the product of values pased in startdt, enddt and geom to establish a finite set of urls to stack. Defers to XArray for file opening and backend driver selection.
    Parameters
    ----------
    urlpath : str
 
    startdt, enddt: datetime
        Start and end dates used to retrieve AODN data and crop final stacked dataset to.
    geom: str 
        Polygon with geographical coordinates (minLon,minLat,minLon,maxLat,maxLon,maxLat,maxLon,minLat,minLon,minLat).
        Some examples:
            - 'POLYGON ((111.0000000000000000 -33.0000000000000000, 111.0000000000000000 -31.5000000000000000, 115.8000030517578125 -31.5000000000000000, 115.8000030517578125 -33.0000000000000000, 111.0000000000000000 -33.0000000000000000))'
    chunks : int or dict, optional

    storage_options: dict
        If using a remote fs (whether caching locally or not), these are
        the kwargs to pass to that FS.
    """
    name = 'refzarr_aodnstack'

    def __init__(self, 
                 urlpath,
                 startdt, 
                 enddt,
                 cropto={},
                 geom="",
                 variables = None,
                 storage_options=None,
                 chunks='auto', 
                 rename_fields=None,
                 metadata=None,
                 **kwargs):
        
        if enddt == pd.Timestamp('1970-01-01 00:00:00'):
            enddt = pd.Timestamp.now()
            # warnings.warn(f'enddt was not specified, defaulting to {enddt}')
        else:
            enddt = pd.to_datetime(enddt)
        
        if startdt == pd.Timestamp('1970-01-01 00:00:00'):
            startdt = enddt - pd.DateOffset(months=1)
            # warnings.warn(f'startdt was not specified, defaulting to {startdt}')
        else:
            startdt = pd.to_datetime(startdt)
            
        if startdt > enddt:
            raise ValueError(f'Invalid dates, startdt:{startdt} is after enddt:{enddt}')
        
        self.urlpath = urlpath
        self.startdt = startdt
        self.enddt = enddt
        self.cropto = cropto
        self.geom = geom
        self.storage_options = storage_options
        self.chunks = chunks
        self.variables = variables
        self.rename_fields = rename_fields 
        self._ds = None
        self._load_data = False
        super(RefZarrStackSource, self).__init__(metadata=metadata, **kwargs)
        

    def to_dask(self):
        """Return xarray object where variables are dask arrays"""
        self._load_data = False
        self._load_metadata()
        return self._ds

    def read(self):
        """Return xarray object where variables are dask arrays"""
        self._load_data = True
        self._load_metadata()
        return self._ds
    
    def print(self):
        from fsspec.core import url_to_fs
        import pandas as pd
        import os
        import json
        import fsspec
        import xarray as xr
        yearmon = sorted(set(pd.date_range(self.startdt,self.enddt,inclusive='left').strftime('%Y%m')))
        logger.info(f'Printing Months {yearmon}')
        # Get the list of json files from the reference zip
        urlparts = self.urlpath.split('::')
        fs, urlpath = url_to_fs(self.urlpath) #,**{self.storage_options['target_protocol']:self.storage_options['target_options']}
        logger.info(f'Scanning {fs} {urlpath}')
        ref_files = fs.glob(urlpath)
        logger.debug(ref_files)
        for f in ref_files:
            # Assume first six digits year month
            ref_file = os.path.basename(f)
            if ref_file[0:6] in yearmon:
                f_url = f'simplecache::zip://{f}::{urlparts[-1]}' #f'{fs.protocol}://{f}::' + '::'.join(urlparts[1:])
                with fsspec.open(f_url) as f: #,**{self.storage_options['target_protocol']:self.storage_options['target_options']}
                    json_payload = json.load(f)
                    mapper=fsspec.get_mapper('reference://',
                                             fo=json_payload,
                                             **self.storage_options)
                    ds = xr.open_zarr(mapper,chunks={'time':14},consolidated=False) 
                    


    def _open_dataset(self):
        def clean_attrs(ds):
            """ remove some attrs that prevent simple save to netcdf """
            for v in ds.variables:
                for bad_attr in ['_Netcdf4Dimid','NAME']:
                    if bad_attr in ds[v].attrs.keys():
                        del ds[v].attrs[bad_attr]
            return ds
        def open_and_crop(fo,
                          storage_options,
                          time=None,
                          cropto=None,
                          varmap=None,
                          load_data=False):



            logger.info(f'fo {fo}')
            logger.info(f'storage_options {storage_options}')

            with fsspec.open(fo) as f: #,**{storage_options['target_protocol']:storage_options['target_options']}
                json_payload = json.load(f)
                mapper=fsspec.get_mapper('reference://',
                                         fo=json_payload,
                                         **{'simple_templates': True, 'target_options': {}, 'target_protocol': 's3', 'remote_options': {'anon': True}, 'remote_protocol': 's3'}) #storage_options
                ds = xr.open_zarr(mapper,chunks={'time':14},consolidated=False) 
                #ds['lat'] = ds.lat[::-1]
                # if self.variables is not None:
                #     ds  = ds[self.variables]
                logger.debug(f'Dataset : {ds}')

                if time is not None:
                    ds = ds.sel(time=time)
                if varmap is not None:
                    ds = ds.rename(varmap)
                if cropto is not None:
                    if isinstance(list(cropto.values())[0],list):
                        logger.info(f'Pointwise: {cropto}')
                        cropto['longitude']=xr.DataArray(cropto['longitude'],dims="points")
                        cropto['latitude']=xr.DataArray(cropto['latitude'],dims="points")
                        ds = ds.sel(**cropto)
                    else:
                        logger.info(f'Slicing: {cropto}')
                        ds = ds.sel(**cropto)

                logger.debug(f'Dataset : {ds}')

                if load_data:
                    import dask
                    with dask.config.set(scheduler='single-threaded'):
                        logger.debug(f'Loading data from {fo}')
                        # try:
                        ds = ds.load() #loads this months data
                        # except Exception as ex:
                        #     print('Load Failed: ' + fo)
                        #     print(ex)
                        #     return None
            
            return ds

        logger.info(self)
        logger.info(f'geom:{self.geom}')

        # Establish crop parameter to pass to open
        # if self.geom:
        #     lon, lat = shapely.wkt.loads(self.geom).exterior.coords.xy
        #     self.cropto['latitude']=slice(max(lat),min(lat))
        #     self.cropto['longitude']=slice(min(lon),max(lon))
        #     self.cropto['method']=None       
        open_kwargs = {}
        open_kwargs['storage_options'] = self.storage_options
        open_kwargs['time'] = slice(self.startdt,self.enddt)
        if hasattr(self.cropto, 'bounds'):
                    crop ={}
                    lonmin,latmin=self.cropto.bounds.min()[['minx','miny']]
                    lonmax,latmax=self.cropto.bounds.max()[['maxx','maxy']]                    
                    crop['latitude']=slice(latmax,latmin)
                    crop['longitude']=slice(lonmin,lonmax)
                    crop['method']=None
                    open_kwargs['cropto'] = crop
        else:
            open_kwargs['cropto'] = self.cropto
        open_kwargs['varmap'] = self.rename_fields
        open_kwargs['load_data'] = self._load_data

        logger.info(f'open_kwargs: {open_kwargs}')

        yearmon = sorted(set(pd.date_range(self.startdt,self.enddt,inclusive='left').strftime('%Y%m')))
        logger.info(f'Loading Months {yearmon}')

        # Get the list of json files from the reference zip
        urlparts = self.urlpath.split('::')
        fs, urlpath = url_to_fs(self.urlpath,**{self.storage_options['target_protocol']:self.storage_options['target_options']})
        logger.info(f'Scanning {fs} {urlpath}')
        ref_files = fs.glob(urlpath)
        logger.debug(ref_files)
        
        d_open_dataset = delayed(open_and_crop)
        futures = []
        loaded_files = []
        for f in ref_files:
            # Assume first six digits year month
            ref_file = os.path.basename(f)
            if ref_file[0:6] in yearmon:
                loaded_files.append(f)
                f_url = f'simplecache::zip://{f}::{urlparts[-1]}' #f'{fs.protocol}://{f}::' + '::'.join(urlparts[1:])
                futures.append(d_open_dataset(f_url,**open_kwargs))
        dsets = compute(futures)[0]
        
        for i,ds in enumerate(dsets):
            if ds is None:
                logger.warning('Failed to open: ' + loaded_files[i])
                
        dsets = [ds for ds in dsets if ds is not None]
        commonvars = list(set.intersection(*list((map(lambda x:set([i for i in x.data_vars]),dsets)))))
        dsets = [ds[commonvars] for ds in dsets]
        xarray_concat_kwargs = dict(dim='time',coords='minimal',join='override',compat='override',combine_attrs='override',data_vars='minimal')        
        ds = xr.concat(dsets, **xarray_concat_kwargs)
        # from shapely.geometry import mapping
        # ds =ds.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude", inplace=True)
        # ds.rio.write_crs("epsg:4326", inplace=True)
        # ds = ds.rio.clip(self.cropto.geometry.apply(mapping), self.cropto.crs, drop=False)
        ds = ds.sortby('time') # The sort is beneficial to re-order the data according to the coordinate, as having separate stacks for the different layouts breaks the ordering.

        if not self._load_data:
            ds = ds.chunk(self.chunks)

        self._ds = clean_attrs(ds)
