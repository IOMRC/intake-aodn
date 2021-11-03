#!/usr/bin/env python
#-----------------------------------------------------------------------------
# Copyright (c) 2020 - 2021, CSIRO 
#
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from setuptools import setup, find_packages

requires = [line.strip() for line in open('requirements.txt').readlines()
            if not line.startswith("#")]

setup(
    name='intake-aodn',
    version='0.0.1a'
    cmdclass=versioneer.get_cmdclass(),
    description='Relocatable Ocean Modelling in PYthon (rompy)',
    url='https://github.com/IOMRC/intake-aodn',
    maintainer='Paul Branson',
    maintainer_email='paul.branson@csiro.au',
    license='BSD',
    py_modules=['intake_aodn'],
    package_data={'': ['*.csv', '*.yml', '*.yaml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    packages=find_packages(),
    entry_points={
        'intake.drivers': [
            'refzarr_stack = intake_aodn.driver:NetCDFFCStackSource'
        ],
        'intake.catalogs': [
            'aodn_data = intake_aodn:cat'
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.6",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    zip_safe=False,
)