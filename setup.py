import pathlib
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()



setup(name='kubemq',
      version='1.0.2',
      description='KubeMQ SDK for Python',
	  long_description=README,
	  long_description_content_type="text/markdown",
      url='https://github.com/kubemq-io/kubemq-Python',
      author='KubeMQ',
      author_email='info@kubemq.io',
      license='MIT',
      packages=[
          'kubemq',
          'kubemq.basic',
          'kubemq.commandquery',
          'kubemq.commandquery.lowlevel',
          'kubemq.events',
          'kubemq.events.lowlevel',
          'kubemq.grpc',
          'kubemq.subscription',
      ],
      install_requires=[
          'future==0.17.1',
          'grpcio==1.17.1',
          'protobuf==3.6.1',
      ],
      zip_safe=False)
