from setuptools import setup

setup(name='kubemq',
      version='1.0.1',
      description='KubeMQ SDK for Python',
      url='http://github.com/KubeMQ/Python_SDK',
      author='KubeMQ',
      author_email='kubemq@kubemq.io',
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
