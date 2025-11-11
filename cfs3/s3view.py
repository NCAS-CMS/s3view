#! /usr/bin/env python
# -*-python-*-

from cfs3.s3cmd import s3cmd
import sys

def main(argv=None):
    """
    Initial arguments:
    - No arguments: We will display your S3 minio locations from your config file.
    - One argument: That will be a minio location from your config file, appliations
        starts with a list buckets on that loation (`lb location`)
    """
    if argv is None:
        argv = sys.argv
    if len(argv) > 2:
        print(main.__doc__)
        exit(1)
    match len(argv):
        case 1:
            c = s3cmd(path = None)
        case 2:
            c = s3cmd(path = argv[1])
    sys_exit_code = c.cmdloop()
    return sys_exit_code


if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv))
    