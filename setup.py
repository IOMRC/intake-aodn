#!/usr/bin/env python
#-----------------------------------------------------------------------------
# Copyright (c) 2020 - 2021, CSIRO 
#
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from os.path import exists
from setuptools import setup
import versioneer

setup(
    name='intake-aodn',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='A collection of intake catalogs and drivers to access AODN data directly in AWS S3',
    url='https://github.com/IOMRC/intake-aodn',
    maintainer='Paul Branson',
    maintainer_email='paul.branson@csiro.au',
    license='BSD',
    py_modules=['intake_aodn'],
    install_requires=list(open('requirements.txt').read().strip().split('\n')),
    packages=['intake_aodn',],
    entry_points={
        'intake.drivers': [
            'refzarr_stack = intake_aodn.drivers:RefZarrStackSource'
        ],
        'intake.catalogs': [
            'aodn_data = intake_aodn:cat'
        ]
    },
    python_requires=">=3.7",
    long_description=(open('README.md').read() if exists('README.md') else ''),
    long_description_content_type="text/markdown",
    zip_safe=True,
)