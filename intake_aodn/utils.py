#!/usr/bin/env python
#-----------------------------------------------------------------------------
# Copyright (c) 2020 - 2021, CSIRO 
#
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

def get_local_cluster():
    import dask
    from distributed import Client, LocalCluster
    dask.config.config.get('distributed').get('dashboard').update({'link':'{JUPYTERHUB_SERVICE_PREFIX}proxy/{port}/status'})
    try:
        client = Client('tcp://localhost:8786', timeout='1s')
    except OSError:
        cluster = LocalCluster(scheduler_port=8786)
        client = Client(cluster)
    return client

def get_distributed_cluster(worker_cores=1,worker_memory=2.0, min_workers=1, max_workers=64):
    from dask.distributed import Client
    from dask_gateway import Gateway

    if worker_cores > 4:
        raise ValueError('Please dont create a worker larger than 4 cores')

    # Dask gateway
    gateway = Gateway()
    clusters = gateway.list_clusters()
    if not clusters:
        print('Creating new cluster. Please wait for this to finish.')
        options = gateway.cluster_options()
        options.worker_cores = worker_cores
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


                


