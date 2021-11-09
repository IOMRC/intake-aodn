# intake-aodn

`intake-aodn` contains early demonstration of 'cloud-native' approaches to extraction and analysis of data curated by IMOS and published on the AODN portal. This demonstration forms part of the Cloud Analytics component of the IMOS New Technology Proving Project - Ocean Observer.

A significant amount of IMOS data is accessible directly on Amazon Web Serices S3 Object store: http://imos-data.s3-website-ap-southeast-2.amazonaws.com/. A significant portion of this data is stored in NetCDF format. A service to query and download of this data is provided by the AODN THREDDS Service: http://thredds.aodn.org.au/thredds/catalog/catalog.html.

Being able to access S3 provides a number of opportunites to leverage open source tools and libraries being developed for cloud computing. Earlier examples include this set of demonstration notebooks presented at [C3DIS in 2019]([https://github.com/pbranson/c3dis-2019-pangeo) and recent examples on the [IMOS blog](https://imos.org.au/news/newsitem/experience-the-breadth-of-web-services-for-imos-data-access-1).

Since NetCDF version 4, NetCDF files utilise the HDF5 file format for the storage of n-dimensional data. HDF5 provide a tree representation that maps blocks of the underlying array data into contiguous (usually compressed) binary chunks stored on disk. The traditional NetCDF drivers were developed for POSIX-compliant filesystems, that typically have low latency and allow for file-locking mechanisms. Such functionality is not provided by an Object Store, where data is provided over stateless HTTP, with higher latency, but potentially much higher throughput. 

Recently, it has been demonstrated that the locations of the binary chunks inside of a HDF file can be mapped, and the binary chunk data accessed directly using the [Zarr](https://github.com/zarr-developers/zarr-python) and [fsspec](https://github.com/fsspec/filesystem_spec) libraries to provide a key/value mapping interface that performs byte range requests directly to S3, see https://medium.com/pangeo/cloud-performant-netcdf4-hdf5-with-zarr-fsspec-and-intake-3d3a3e7cb935. This work has continued in the kerchunk library https://github.com/fsspec/kerchunk (formerly fsspec-reference-maker).  

`intake-aodn` uses the kerchunk library to map the binary chunk layout of the several IMOS Satellite Remote Sensing Facility datasets, which are valuable repositories of 30 years of Sea Surface Temperature and 20 years of Ocean Colour for the Australian Region. Using this approach retrieval of a time series at a single pixel across the stacks of thousands of NetCDF files has been demonstrated to be possible in under a minute, compared to several hours via the AODN THREDDS service. 

There is (a growing!) collection of intake catalogs and drivers that provide access to the AODN data directly in AWS S3 and demonstration notebooks that show how to utilise the catalogs and drivers. All binary chunk references are stored in this repository in a zip file, so that the approach can be utilised from anywhere. 

NOTE: The bandwidth utilisation for a 'pixel-drill' is high, as to retrieve a single pixel, a complete chunk must be downloaded and uncompressed which may be several MB per time point. Therefore it is best to run these notebooks from AWS, or from a network with good connectivity to AWS. 

Contributors: \
[@pbranson](https://github.com/pbranson) \
[@NickMortimer](https://github.com/NickMortimer) \
[@maximemarin](https://github.com/maximemarin) \
[@dirkslawinski](https://github.com/dirkslawinski)
