#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

requirements = []

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest>=3', ]

setup(
    author="Wataru Hirota",
    python_requires='>=3.8',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.8',
    ],
    description='A type-aware workflow engine working on Python',
    install_requires=requirements,
    license="MIT license",
    include_package_data=True,
    keywords='typedflow',
    name='typedflow',
    packages=find_packages(where='.'),
    package_data={
        'typedflow': ["py.typed"],
    },
    setup_requires=setup_requirements,
    test_suite='typedflow.tests',
    tests_require=test_requirements,
    url='https://github.com/tarohi24/typedflow',
    version='0.1.0',
    zip_safe=False,
)
