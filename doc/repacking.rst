Repacking 
---------

Whether one wants to use Zarr or HDF5/Netcdf4, it is often necessary to rechunk datasets, particularly
when those datasets are geophysical observations or simulations which were originally written
one time-step at a time, with an unlimited time dimension.  Rechunking involves reading the 
dataset as is, and writing it with a new chunking strategy. Possible chunking strategies
are discussed in the documentatino for 