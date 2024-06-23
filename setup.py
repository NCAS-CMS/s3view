from setuptools import setup, find_packages
setup(
    name='mycommand',
    version='0.1',
    author='Your Name',
    author_email='your@email.com',
    description='My Command Line Tool',
    packages=find_packages(),
    scripts=['s3v/s3v'], 
    install_requires = [
        'cmd2>=1,<2',
        'pyreadline3;platform_system=="Windows"'
        ]
    )