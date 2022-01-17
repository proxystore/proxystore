"""Build ProxyStore package."""
import setuptools

with open("README.md") as f:
    long_desc = f.read()

setuptools.setup(
    name="ProxyStore",
    version="0.3.1",
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
    python_requires=">=3.7",
    install_requires=[
        "lazy-object-proxy>=1.6.*",
        "cloudpickle>=1.6.0",
    ],
)
