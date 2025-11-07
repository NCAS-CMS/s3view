Object Store Basics
*************************

Most of us don't need or want to understand the difference between storage systems, but sometimes we can't 
avoid some knowledge of how they work. This section provides a brief overview of object stores, and contrast
them with traditional file systems.

There are two key distinctions in practice:

- Object stores are designed for remote access, using HTTP-based protocols, whereas traditional
  file systems are designed for local access.
- Object stores manage data as discrete immutable objects, each with its own identifier (key).


These two distinction have important important implications for performance and usability which when
ignored lead to confusion and frustration - particularly in the context of compressed and chunked
weather and climate data in formats like NetCDF and HDF5.  It  can be argued that many of the 
reasons why people argue that Zarr is the best format for object stores is precisely because
people haven't fully understood these distinctions (and their implications for the use of Zarr
on other storage systems).

In this section we briefly expand on these two distinctions, describe some key aspects of scientific
data formats, and then discuss the implications
for using and managing scientfic data on object stores. The write-up targets some of the assumptions
we make about storage in the hope that knowing what is going on will help people make decisions about
how to use data storage in efficient ways.

Remote Access
-----------------

Traditional file systems are designed for local access, where the operating system can directly read and write
files on disk. This allows for low-latency access to data, as the OS can quickly locate and retrieve not 
only the files themselves, but also support relatively rapid random access within files. By contrast, object stores
are designed for remote access over networks, and an assumption that users will generally obtain entire objects.

Immutable objects
-----------------------

In traditional file systems, files can be modified in place. This allows for efficient updates to data, as only
the changed portions of a file need to be written. Object stores, on the other hand, treat objects as immutable; 
to modify an object, a new version must be created and the old version discarded. This design simplifies data 
management and enables features like versioning and easy replication across distributed systems - but it means
that we can't add more information to a file without rewriting the entire file.

File Formats - NetCDF/HD5 and Zarr
----------------------------------

Much of the data we are interested in is stored in formats which compress the data into chunks. This chunking
has many benefits, in that it means that when we access part of a dataset we only need to read (and decompress) 
the relevant chunks. When those chunks are stored together in a file, that means we depend on random
access into the chunks, and whether or not the chunks are all in the same file, on reading the chunk index first. 

One of the reasons why Zarr is so popular with those putting scientific data on object stores is that the chunk
index is itself a file, and (until recently) each chunk was a file in it's own right.  This layout works
well on object stores, but not well (for large datasets) on traditional file systems. As a consequence the 
latest versions of Zarr allow multliple chunks per file, but a dataset is stil distrubuted over multliple
files with an independent index.

By contrast, NetCDF (and HDF5) store multiple variables, each with multiple chunks per file, and the chunk 
indexes  for each variable are embedded in the file too - and, often these chunk indexes (one for for each
variable or dataset) are distributed throughout the file in structures called "b-tree"s.

Consequences
-------------

When the chunk indices are on a local file system with the chunks, whether in different files, or in
the same file, access speeds are fast - reading the index, and then finding the chunk itself are
activities which occur on timescales measured in milliseconds. By contrast, when reading from
an object store, each action can take  seconds (depending on how close the object store is to the
client). 

The chunking strategy used when storing the data is an important constraint on performance, but
so to is the reading of the index itself. Clearly reading a chunk index in one go (as happens with Zarr) 
is as efficient as it can be, but reading a distributed index can take a long time, as one needs to 
read each part of the b-tree before knowing where to find the next bit.  Middleware like ``fsspec`` 
mitigate some of this by clever use of caching and readahead, but this index parsing job can be very
difficult for NetCDF4 and HDF5.  

There are three mitigation strategies for improving performance on object stores: 

1. use of parallel reads, 
2. good use of chunking, and 
3. consolidating the b-tree indices togehter in one place.  

Many advocates of Zarr have conflated thes use of Zarr with the use of these mitigation strategies: 
for sure, using Zarr means thatone can have a pure-python stack suitable for parallelism, that one has 
probably had to rechunk the data, and one gets a consoldiated index by default, but these mitigation 
strategies are also available to those who do not want to use Zarr and keep using NetCDF4 and HDF5
(although the first has only been available in threaded situations with recent upgrades to ``pyfive``.)

Implications for cfs3
--------------------------

There are currently two sets of tools within this package:

- ``s3view`` provides a way to discover and explore data stored in object stores, with a focus on exploiting 
  the metadata stored within the files in an efficient was as possible by caching information between
  calls and  exploiting parallelism to speed up access. The aim in doing  is to mimic the sort of information
  one would get if someone had already built a catalog of the data contents (or indeed to build a catalog).
- ``cftools`` provides some python classes which can be used to process data files so that they
  include the metadata which supports the efficient access patterns used by ``s3view`` (and the
  information necessary to build catalogs).  These tools also include utilities to help with
  the rechunking of data to make it more suitable for access on object stores.

One of the other important distinctions between object stores and traditional file systems is that object stores
also include extra metadata services, allowign one to store additional information about each object - typically
via a) key-value pairs attached to each object when it is uploaded and/or b) tags which can be
attached to objects after upload.  Unfortunately, not all object stores support both of these, and
without additional software infrastructure, traditional file systems support neither. However, cftools
include utilities to help add such metadata to objects when they are uploaded to object stores, and
s3view is able to exploit such metadata when it is present.  