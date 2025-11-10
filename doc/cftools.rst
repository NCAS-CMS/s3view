.. _cftools:

cftools
*******

To properly exploit object stores there some key steps to implement
the performance mitigations outlined in :ref:`object_stores`:

1. We need to ensure that there is only one variable per file.
2. The variable is sensible chunked, and the chunk index is at the front of the file, and
3. The file is uploaded with object store metadata.

The ``cftools`` packages provide classes that can be incorporated in 
user workflows to achieve these outcomes.


.. autoclass:: cfs3.CFSplitter
   :members:

.. _cfuploader: 

.. autoclass:: cfs3.CFuploader
   :members:

.. autoclass:: cfs3.MetaFix
   :members:

.. autoclass:: cfs3.FileNameFix
   :members:

Context
----------

.. toctree::
   :maxdepth: 1

   chunking
   repacking

