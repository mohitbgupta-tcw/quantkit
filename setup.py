from setuptools import setup, find_packages
import os


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="Quantkit Version 1.3.1",
    version="1.3.1",
    author="Tim Bastian",
    author_email="tim.bastian@tcw.com",
    url="https://gitlab.com/tcw-group/quant-research/quantkit",
    description=("TCW's quantkit package"),
    long_description=read("README"),
    packages=find_packages(),
)
