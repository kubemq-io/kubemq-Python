import pathlib
from setuptools import setup, find_packages  # Corrected import statement

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

setup(
    name="kubemq",
    version="3.5.0",
    description="KubeMQ SDK for Python",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/kubemq-io/kubemq-Python",
    author="KubeMQ",
    author_email="info@kubemq.io",
    license="MIT",
    packages=find_packages(),  # Corrected function call
    install_requires=[
        "grpcio==1.71.0",
        "protobuf>=4.21.0",
        "setuptools>=40.8.0",
        "PyJWT>=2.6.0",
        "pydantic",
    ],
    zip_safe=False,
)
