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
        raise ValueError('Please dont create ')

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



