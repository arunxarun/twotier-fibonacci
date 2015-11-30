#!/usr/bin/env python

from setuptools import setup, find_packages


def read(relative):
    with open(relative, 'r') as fin:
        contents = fin.read()
        return [l for l in contents.split('\n') if l != '']

setup(
  name='twotier-fibonacci',
  version='0.1',
  description='fibonacci broken out between web and worker tier',
  author='Arun Jacob',
  author_email='arunxarun@github.com',
  url='https://github.com/arunxarun/twotier-fibonacci',
    tests_require=read('./requirements.pypm'),
    install_requires=read('./requirements.pypm'),
    test_suite='nose.collector',
    include_package_data=True,
    packages=find_packages(exclude=['*.tests']))
