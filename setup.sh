#!/bin/sh
cd src
py -3 setup.py develop
py -3 -m pip install -e .