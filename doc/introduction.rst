Introduction
***************

Object stores have properties which differ from traditional file systems (see :ref:`object_stores`), this package
provides a command line utility and some library functions. which exploit a combination of the CF
standards (http://www.cfconventions.org) and external metadata which conform to a :ref:`DRS`, to aid in 
efficient use of object stores.

Background
--------------

More detail on object stores and metadata are described here:

.. toctree::
   :maxdepth: 2
   
   object_stores
   metadata

Tools and software
--------------------


There are currently two sets of tools within this package:

- :ref:`s3view` is a commmand line utility which provides a way to discover and explore data stored in object stores, 
  with a focus on exploiting the metadata stored within the files in an efficient was as possible by caching
  information between calls and  exploiting parallelism to speed up access. The aim in doing is to mimic the 
  sort of information one would get if someone had already built a catalog of the data contents 
  (or indeed to build a catalog).

- :ref:`cftools` is a library provides some python classes which can be used to process data files so that they
  include the metadata which supports the efficient access patterns used by ``s3view`` (and the
  information necessary to build catalogs).  These tools also include utilities to help with
  the rechunking of data to make it more suitable for access on object stores.


