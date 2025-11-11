# cfs3

Package of tools for manipulating data held in S3 which can make use of the CF compliant metadata in the files,  and use, if it exists extra information conforming to
a user defined  "Data Reference Syntax" (DRS) which covers some structured requirements for data and file attributes and filenames.

For now, you need to git clone it, and install from the cloned repository using `pip install --use-pep517 -e .`

You will almost certainly need to configure your access details using a minio configuration file as described below.

## s3view

Lightweight tool for navigating around an S3 repository, utilising minio tools (hence, to use it, you need to set up a minio configuration file (`config.json`) in your `~./mc` directory. See below.)

To run it, simply run the script `s3view`, and there is help within that.

Currently commands supported are:
 - `loc` (for changing minio locations)
 - `cb` (for changing buckets)
 - `cd`, `ls`, `rm`  (all do what you would expect)
 - `mv` (only lets you rename stuff within a bucket. Note that this is expensive as it really involves a copy. Try not to need to do it!)

Note that unlike other packages, we treat each bucket as it's own independent file system. We could tream them like top level directories, but we don't. For now.

Yes we know there are easier ways to do most of this (the minio command line tools for example), but this package does different things by default, and we want to be able to use elements of it in other code. We may well extend it to be a bit more "climate data aware" as well. Early days.

### Testing and Reliability

The unittest framework is not yet ready, so it's very much caveat emptor for now. 

### JASMIN Access

For those of you using this on JASMIN, the procedure to get going on the object store is

1. You need to have applied for, and got access to one or more tenancies (in the example below, I'll show you how I got set up for the `hiresgw` tenany, you will need to do this for each and every S3 tenancy you have access to).

2. Create your secret key here: https://s3-portal.jasmin.ac.uk/object-store/hiresgw-o/create-keys. Note that you need a different secret key for each object store tenancy.

Look out for the use of quotes _in_ your secret key, you will have to escape them in your configuration files. E.g. if your secret key looks like `gubbins_"_moregubbins` you will need to enter it in your config files as 
``gubbins_\"_moregubbins``

3. Do your minio configuration as below.

## Minio Configuration

If you are working inside JASMIN and from outside JASMIN, you can set up your minio configuration files in each place so that
your code is complete agnostic about where you are, it just "gets it right".

In this case, you will want to copy two versions of your config and secret key into your `.mc/config.json`, one for JASMIN itself, and one for your (remote) sites (e.g. RACC, laptop etc).

Mine look like:

- at JASMIN:
```json
"hrs3":{
            "url": "http://hiresgw-o.jc.rl.ac.uk",
			"accessKey": "Stuff",
			"secretKey": "Longer \" Stuff",
			"api": "S3v4",
      	    "path": "auto"    
		},
```
and elsewhere:
```json
"hrs3":{
            "url": "https://hiresgw-o.s3-ext.jc.rl.ac.uk",
			"accessKey": "Stuff",
			"secretKey": "Longer \" Stuff",
			"api": "S3v4",
      	    "path": "auto"    
		},
```
Note the workspace is `hiresgw-o` in both cases, but it is `https` in one and `http` in the other, and the `s3-ext` in the elsewhere one. Note also that this is just one part of the minio configuration file, you will have multiple entries in your file, which will
then look something like this:

```json
{
        "version": "10",
        "aliases": {
            "hrs3": { ...},
            "another": { ...}
        }
}
```
