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
    from distributed import Client
    dask.config.config.get('distributed').get('dashboard').update({'link':'{JUPYTERHUB_SERVICE_PREFIX}proxy/{port}/status'})
    client=Client()
    return client

def get_distributed_cluster(worker_cores=1,worker_memory=2.0):
    from dask.distributed import Client
    from dask_gateway import Gateway

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
    cluster.adapt(minimum=1, maximum=64)
    client = cluster.get_client()
    client.wait_for_workers(n_workers=1)
    return client



