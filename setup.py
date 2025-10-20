import pathlib
from setuptools import setup, find_packages  # Corrected import statement

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

setup(
    name="kubemq",
    version="3.6.0",
    description="KubeMQ SDK for Python",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/kubemq-io/kubemq-Python",
    author="KubeMQ",
    author_email="info@kubemq.io",
    license="MIT",
    python_requires=">=3.9",
    packages=find_packages(),  # Corrected function call
    install_requires=[
        "grpcio>=1.71.0,<2.0.0",
        "protobuf>=4.21.0,<7.0.0",
        "setuptools>=65.5.0",
        "PyJWT>=2.6.0,<3.0.0",
        "pydantic>=2.0.0,<3.0.0",
    ],
    zip_safe=False,
)
