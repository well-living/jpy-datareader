#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JPy DataReader

@author: WeLLiving@well-living
"""

from setuptools import setup, find_packages

setup(
    name="jpy_datareader",
    version="0.0.3",
    description="Remote data access to government data for pandas.",
    author="well-living",
    license="MIT",
    packages=find_packages(),  # "fpy_datareader"
    classfiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3.8",
    ],
    install_requires=["numpy", "pandas", "requests"],
)
