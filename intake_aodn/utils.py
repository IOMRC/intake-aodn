#!/usr/bin/env python
#-----------------------------------------------------------------------------
# Copyright (c) 2020 - 2021, CSIRO 
#
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

def get_local_cluster(n_workers=8,threads_per_worker=1):
    import dask
    from distributed import Client, LocalCluster
    dask.config.config.get('distributed').get('dashboard').update({'link':'{JUPYTERHUB_SERVICE_PREFIX}proxy/{port}/status'})
    try:
        client = Client('tcp://localhost:8786', timeout='1s')
    except OSError:
        cluster = LocalCluster(n_workers=n_workers,threads_per_worker=threads_per_worker,scheduler_port=8786)
        client = Client(cluster)
    return client

def get_distributed_cluster(worker_cores=1,worker_memory=2.0, min_workers=1, max_workers=64, worker_threads=None):
    from dask.distributed import Client
    from dask_gateway import Gateway
    
    if worker_threads is None:
        worker_threads = worker_cores

    if worker_cores > 8:
        raise ValueError('Please dont create a worker larger than 4 cores')

    # Dask gateway
    gateway = Gateway()
    clusters = gateway.list_clusters()
    if not clusters:
        print('Creating new cluster. Please wait for this to finish.')
        options = gateway.cluster_options()
        options.worker_cores = worker_cores
        options.worker_threads = worker_threads
        options.worker_memory = worker_memory
        cluster = gateway.new_cluster(cluster_options=options)
    else:
        print(f'An existing cluster was found. Connected to cluster \033[1m{clusters[0].name}\033[0m')
        cluster=gateway.connect(clusters[0].name)
    cluster.adapt(minimum=min_workers, maximum=max_workers)
    client = cluster.get_client()
    client.wait_for_workers(n_workers=1)
    display(cluster)
    return client

def display_entry(entry):
    import pandas as pd #
    print(f'NAME: {entry.name}')
    print(f'DESCRIPTION: {entry.description}')
    print(f'TYPE: {entry.container}')
    print(f'METADATA: ')
    for k,v in entry.metadata.items():
       print(f'    {k} : {v}') 
    print('USER PARAMETERS:')
    user_params = pd.DataFrame(entry.describe()['user_parameters'])
    display(user_params)
    
def get_list_datasets(cat):
    global da_list
    da_list = []
    global ser_list 
    ser_list = []
    da_ser = ["aodn_s3","nci"]
    for ser in da_ser:
        for entry in cat[ser]:
            da_list.append(cat[ser][entry].name)
            ser_list.append(ser)
        
    return da_list,ser_list

def get_default_time(entry):
    from datetime import datetime
    import pandas as pd
    
    param = entry.describe()['user_parameters']
    info = list(filter(lambda mini: mini['name']=='startdt',param))[0]
    startdt = info['min'].date().strftime('%Y-%m-%d')
    enddt = info['max'].date().strftime('%Y-%m-%d')
    return startdt, enddt

def dw_data(dataset,coord,time_start=None,time_end=None,load_type = 'read'):
    import intake_aodn
    #from datetime import datetime
    from intake_aodn.utils import display_entry 
    from intake_aodn.utils import get_list_datasets
    from intake_aodn.utils import get_default_time
    from datetime import datetime

    
    catal = intake_aodn.cat
    da_list,ser_list = get_list_datasets(catal)  
    lat = coord[1]
    lon = coord[0]
    
    [startdt,enddt] = get_default_time(eval('intake_aodn.cat.' + ser_list[da_list.index(dataset)] + '.' + dataset))
    
    if time_start is None:
        time_start = startdt
    elif datetime.strptime(time_start,'%Y-%m-%d')<datetime.strptime(startdt,'%Y-%m-%d'):
        raise ValueError('start time: %s is before the minimum date: %s' % (datetime.strptime(time_start,'%Y-%m-%d'),startdt))
        
    if time_end is None:
        time_end = enddt
    elif datetime.strptime(time_end,'%Y-%m-%d')<datetime.strptime(enddt,'%Y-%m-%d'):
        raise ValueError('end time: %s is after the maximum date: %s' % (datetime.strptime(end_start,'%Y-%m-%d'),enddt))
    
    if isinstance(lat,float) and isinstance(lon,float): #point
        argum = "(startdt='" + time_start + "',enddt='" + time_end + "',cropto=dict(latitude=lat,longitude=lon,method = 'nearest'))." + load_type + "()"

    elif isinstance(lat,list) and isinstance(lon,list): #box
        lat.sort()# note that for slice, index order is necessary rather than coordinate order. In this product, latitude is indexed from high to low values
        lon.sort()# this might have to be modified for datasets which list latitude in increasing order.
        
        argum = "(startdt='" + time_start + "',enddt='" + time_end + "',cropto=dict(latitude=slice(lat[1],lat[0]),longitude=slice(lon[0],lon[1])))." + load_type + "()" 
    else:
        print('Coordinates not a point or orthogonal box')
        
    ds  = eval('intake_aodn.cat.' + ser_list[da_list.index(dataset)] + '.' + dataset + argum)
    return ds

def save_netcdf(data,filename):
# Need to drop the "NAME" and "_Netcdf4Dimid" which are reserved when writing back out to netCDF
# Unclear why they are in the dataset
            
    # Unclear why they are in the dataset
    for v in data.variables:
        for bad_attr in ['_Netcdf4Dimid','NAME']:
            if bad_attr in data[v].attrs.keys():
                del data[v].attrs[bad_attr]

    data.to_netcdf(filename)
 

def save_excel(df_list,file_name = "myfile",*args):
# args can contain a list of the name of sheets (sheetname)
# large spatial daily files can take 10-20mins
    if 'sheetname' in args:
        sheetname = args["sheet_name"]
    else:
        sheetname = list(str(range(1,len(df_list)+1)))
        
    with pd.ExcelWriter(file_name+'.xlsx') as writer:
    
        for i in range(0,len(df_list)):
            if df_list[i].shape[0]>=2**20:
                print("Warning: data #" +str(i)+ " is too large to save as a .xlsx file")
            else:
                df_list[i].to_excel(writer,sheet_name=str(sheetname[i]))


                


