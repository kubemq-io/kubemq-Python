import pathlib
from setuptools import setuptools

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()



setuptools.setup(name='kubemq',
      version='2.2.3',
      description='KubeMQ SDK for Python',
	  long_description=README,
	  long_description_content_type="text/markdown",
      url='https://github.com/kubemq-io/kubemq-Python',
      author='KubeMQ',
      author_email='info@kubemq.io',
      license='MIT',
      packages=setuptools.find_packages(),
      install_requires=[
          'grpcio>=1.27.2',
          'protobuf>=3.13.0',
          'setuptools>=40.8.0',
          'PyJWT>=1.7.1',
      ],
      zip_safe=False)
