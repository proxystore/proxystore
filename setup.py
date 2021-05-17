"""Build ProxyStore package"""
import setuptools

with open('requirements.txt') as f:
    install_requires = f.readlines()

with open('README.md') as f:
    long_desc = f.read()

setuptools.setup(
    name="ProxyStore",
    version="0.2.0",
    author="Greg Pauloski",
    author_email="jgpauloski@uchicago.edu",
    description="Python Lazy Object Proxy Interface for Distributed Stores",
    long_description=long_desc,
    url="https://github.com/gpauloski/ProxyStore",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=install_requires,
)
