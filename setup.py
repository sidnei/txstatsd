from distutils.core import setup
from glob import glob
import os

from txstatsd import version

# If setuptools is present, use it to find_packages(), and also
# declare our dependency on epsilon.
extra_setup_args = {}
try:
    import setuptools
    from setuptools import find_packages
except ImportError:
    def find_packages():
        """
        Compatibility wrapper.

        Taken from storm setup.py.
        """
        packages = []
        for directory, subdirectories, files in os.walk("txstatsd"):
            if '__init__.py' in files:
                packages.append(directory.replace(os.sep, '.'))
        return packages

long_description = """
Twisted-based implementation of a statsd-compatible server and client.
"""


setup(
    name="txStatsD",
    version=version.txstatsd,
    description="A network daemon for aggregating statistics",
    author="txStatsD Developers",
    url="https://launchpad.net/txstatsd",
    license="MIT",
    packages=find_packages(),
    scripts=glob("./bin/*"),
    long_description=long_description,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Intended Audience :: Information Technology",
        "Programming Language :: Python",
        "Topic :: Database",
        "Topic :: Internet :: WWW/HTTP",
        "License :: OSI Approved :: MIT License",
       ],
    **extra_setup_args
    )
