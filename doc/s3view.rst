.. _s3view: 

s3view
***********

``s3view`` is a command line tool for exploring data stored in object stores which expose their
data via the S3 protocol.  

It is designed to provide a way to explore and discover data stored in object stores, with a focus on
exploiting the metadata stored within the files in an efficient was as possible by utilising their 
known structure, by  caching information between calls and by exploiting parallelism to speed up access. 

.. autocmd2:: cfs3.s3cmd
   :members: