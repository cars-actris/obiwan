#!/usr/bin/env python

from setuptools import setup
import os
import re
import io

# Read the long description from the readme file
with open("readme.rst", "rb") as f:
    long_description = f.read().decode("utf-8")


# Read the version parameters from the __init__.py file. In this way
# we keep the version information in a single place.
def read(*names, **kwargs):
    with io.open(
            os.path.join(os.path.dirname(__file__), *names),
            encoding=kwargs.get("encoding", "utf8")
    ) as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


# Run setup
setup(name='obiwan',
      packages=['obiwan', 'obiwan.lidarchive'],
      version=find_version("obiwan", "__init__.py"),
      description='Package for automated lidar data processing using the Single Calculus Chain',
      long_description=long_description,
      url='',
      author='Victor Nicolae',
      author_email='victor.nicolae@inoe.ro',
      license='MIT',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 2',
          'Intended Audience :: Science/Research',
          'Topic :: Scientific/Engineering :: Atmospheric Science',
      ],
      keywords='lidar licel',
      install_requires=[
        "atmospheric_lidar",
        "scc_access==0.11.0"
      ],
      entry_points={
          'console_scripts': ['obiwan = obiwan.obiwan:main',],
      },
      )
