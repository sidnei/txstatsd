# Copyright (C) 2011-2012 Canonical Services Ltd
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

from distutils.command.install import install
from glob import glob
import os

from txstatsd import version

# If setuptools is present, use it to find_packages()
extra_setup_args = {}
try:
    import setuptools
    from setuptools import find_packages
    from setuptools import setup
except ImportError:
    from distutils.core import setup

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


class TxPluginInstaller(install):
    def run(self):
        install.run(self)
        # Make sure we refresh the plugin list when installing, so we know
        # we have enough write permissions.
        # see http://twistedmatrix.com/documents/current/core/howto/plugin.html
        # "when installing or removing software which provides Twisted plugins,
        # the site administrator should be sure the cache is regenerated"
        from twisted.plugin import IPlugin, getPlugins

        list(getPlugins(IPlugin))

setup(
    cmdclass = {'install': TxPluginInstaller},
    name="txStatsD",
    version=version.txstatsd,
    description="A network daemon for aggregating statistics",
    author="txStatsD Developers",
    url="https://launchpad.net/txstatsd",
    license="MIT",
    packages=find_packages() + ["twisted.plugins"],
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

