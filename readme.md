# S3view

Lightweight tool for navigating around an S3 repository, utilising minio tools (hence, to use it, you need to set up a minio configuration file (`config.json`) in your `~./mc` directory.)

For now, you need to git clone it, and install from the cloned repository using `pip install --use-pep517 -e .`

To run it, simply run the script `s3view`, and there is help within that.

Currently commands supported are:
 - `loc` (for changing minio locations)
 - `cb` (for changing buckets)
 - `cd`, `ls`, `rm`  (all do what you would expect)
 - `mv` (only lets you rename stuff within a bucket. Note that this is expensive as it really involves a copy. Try not to need to do it!)

Note that unlike other packages, we treat each bucket as it's own independent file system. We could tream them like top level directories, but we don't. For now.

Yes we know there are easier ways to do most of this (the minio command line tools for example), but this package does different things by default, and we want to be able to use elements of it in other code. We may well extend it to be a bit more "climate data aware" as well. Early days.


## Testing and Reliability

The unittest framework is not yet ready, so it's very much caveat emptor for now. 
