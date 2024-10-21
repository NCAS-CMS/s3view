from setuptools import setup, find_packages
setup(
    name='s3v',
    version='0.1',
    author='Bryan Lawrence',
    author_email='bryan.lawrence@ncas.ac.uk',
    description='Lightweight tool for investing data files in an s3 repository',
    packages=find_packages(),
    scripts=['s3v/s3view'], 
    install_requires = [
        'cmd2>2',
        'cf-python>=3.16.2',  # 3.17 n/a yet on PyPI
        'minio',
        'pyreadline3;platform_system=="Windows"'
        ]
    )
