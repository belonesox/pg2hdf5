#!/usr/bin/env python
# -*- coding: utf-8 -*-

# This file is part of pg2pandas.
# https://github.com/belonesox/pg2hdf5

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2018, Stas Fomin <stas-fomin@yandex.ru>

from setuptools import setup, find_packages
from pg2hdf5 import __version__

tests_require = [
    'mock',
    'nose',
    'coverage',
    'yanc',
    'preggy',
    'tox',
    'ipdb',
    'coveralls',
    'sphinx',
]

setup(
    name='pg2hdf5',
    version=__version__,
    description='Fast and optimized loading of large HDF5 Dataset from postgres DB',
    long_description='''
Fast and optimized loading of large HDF5 Dataset from postgres DB
''',
    keywords='HDF5 Postgres',
    author='Stas Fomin',
    author_email='stas-fomin@yandex.ru',
    url='https://github.com/belonesox/pg2hdf5',
    license='MIT',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: Unix',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Operating System :: OS Independent',
    ],
    packages=find_packages(),
    include_package_data=False,
    install_requires=[
        "psycopg2",
        # "pandas",
        "numpy",
        # add your dependencies here
        # remember to use 'package-name>=x.y.z,<x.y+1.0' notation (this way you get bugfixes)
    ],
    extras_require={
        'tests': tests_require,
    },
    entry_points={
        'console_scripts': [
            # add cli scripts here in this form:
            # 'pg2pandas=pg2pandas.cli:main',
        ],
    },
)
