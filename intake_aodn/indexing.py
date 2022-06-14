#!/usr/bin/env python
#-----------------------------------------------------------------------------
# Copyright (c) 2020 - 2021, CSIRO 
#
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------


def process_single(url):
    import fsspec
    from kerchunk.hdf import SingleHdf5ToZarr
    
    s3_fn = 's3://' + url
    with fsspec.open(s3_fn, 
                     anon=True, 
                     mode='rb', 
                     default_fill_cache=False, 
                     default_cache_type='none') as f:
        zarr_dict = SingleHdf5ToZarr(f, s3_fn, spec=1, inline_threshold=100).translate()
        
    return zarr_dict   

def open_single(fn,preprocess=None,storage_options=dict(anon=True)):
    import fsspec
    import xarray as xr
    
    mapper=fsspec.get_mapper('reference://',
                             fo=fn,
                             remote_protocol='s3',
                             remote_options=storage_options,
                            )
    ds = xr.open_zarr(mapper,chunks={}, consolidated=False, decode_times=False)   
    
    if preprocess is not None:
        ds = preprocess(ds)
    
    return ds

def keep_fields(fields):
    """Return filter function that keeps variables in the list"""
    fields = fields
    # keep the fields that we want!
    def preproc(refs):
        for k in list(refs):
            drop = True
            for field in fields:
                if k.startswith(field):
                    drop=False
            if drop:
                refs.pop(k)
        return refs
    return preproc


def process_aggregate(root='imos-data/IMOS/SRS/SST/ghrsst/L3S-1d/ngt/',
                       year='2021',
                       month='07',
                       mask='{year}/{year}{month}',
                       dest='../../intake_aodn/catalogs/',
                       suffix='-ABOM-L3S_GHRSST-SSTskin-AVHRR_D-1d_night',
                       extension='nc',
                       check_chunking=None,
                       preprocess=None,
                       storage_options=dict(anon=True),
                       dask=False
                      ):
    import zipfile
    import fsspec
    import json
    import os
    from kerchunk.combine import MultiZarrToZarr
    
    fs = fsspec.filesystem('s3',use_listings_cache=False,**storage_options)
    mask = mask.format(year=year,month=month)
    globstr = f"s3://{root}{mask}*{suffix}.{extension}"
    urls = fs.glob(globstr)
    
    print(f'Aggregating {globstr} - {len(urls)} found.')
        
    if len(urls) >= 1:
        so = dict(
            protocol='s3',
            profile='default', 
            default_fill_cache=False, 
            default_cache_type='first',
        )
        
        print('Loading references...')
        if dask:
            print('... using dask ...')
            from dask import delayed, compute
            d_process_single = delayed(process_single)
            futures = [d_process_single(u) for u in urls]
            ref_dicts = compute(futures)[0]
        else:
            from tqdm import tqdm 
            ref_dicts = []
            for u in tqdm(urls):
                ref_dicts.append(process_single(u))
        
        print('Checking chunk layout...')
        #Deal with different chunk sizes - create a separate aggregate file for each chunking layout
        chunking = {}
        if check_chunking is not None:
            for r in ref_dicts:
                ds=open_single(r,storage_options=storage_options)
                key = ds[check_chunking].chunks
                if key in chunking.keys():
                    chunking[key].append(r)
                else:
                    chunking[key] = [r,]
        else:
            chunking['auto'] = []
            for r in ref_dicts:
                chunking['auto'].append(r)
            
        #Label each set with a, b, c, ...
        labels = [chr(i) for i in range(97,97+len(chunking.keys()))]
        agg_files=[]
        for i, (chunks, refs) in enumerate(chunking.items()):
            
            #setup output location
            agg_file = f"{root}{year}{month}{suffix}_{labels[i]}.json"
            out_file = os.path.join(dest,agg_file)
            print(f'Aggregating into {out_file}')
            
            output = None
            if len(refs) == 1: # Only one refence in this set... just use the source reference file
                output = json.dumps(refs[0]).encode()
            else: # otherwise join the references into one file
                
                mzz = MultiZarrToZarr(
                    refs,
                    remote_protocol="s3",
                    remote_options=storage_options,
                    concat_dims=["time"], coo_map={"time": "data:time"},
                    preprocess=preprocess
                )
                
                try:
                    dict_agg = mzz.translate()
                    output = json.dumps(dict_agg).encode()
                except NotImplementedError as ex:
                    agg_file = f'ERROR(CHUNK): {agg_file} {str(ex)}'
                except Exception as ex:
                    agg_file = f'ERROR(UNKOWN): {agg_file} {str(ex)}'
                    raise ex
                    
            if not output is None:
                with open(out_file,"wb") as outf:
                    outf.write(output)
                agg_files.append(agg_file)
            
        return {mask: agg_files}
    else:
        return {mask: 'ERROR(NOFILES)'}
    
def zip_references(src, dst, arcname=None):
    import zipfile
    zip_ = zipfile.ZipFile(dst, 'w')
    for i in range(len(src)):
            zip_.write(src[i], os.path.basename(src[i]), compress_type = zipfile.ZIP_DEFLATED)
    zip_.close()