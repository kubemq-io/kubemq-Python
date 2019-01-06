# KubeMQ SDK for Python

The **KubeMQ SDK for Python** enables Python developers to easily work with [KubeMQ](https://kubemq.io/). 

## Getting Started

### Prerequisites

KubeMQ-SDK-Java works with **Python 2.7** or newer.

### Installing
 
The recommended way to use the SDK for Python in your project is to consume it from pip.

```
pip install kubemq
```

## Generating Documentation

Sphinx is used for documentation. Use the Makefile to build the docs, like so:

```
$ pip install -r requirements-docs.txt
$ cd docs
$ make html
```
(`make latex` or `make linkcheck` supported)

## Building from source

Once you check out the code from GitHub, you can install the package locally with:

```
$ pip install .
```

You can also install the package with a symlink, 
so that changes to the source files will be immediately available:

```
$ pip install -e .
```

## Running the examples

The [examples](https://github.com/KubeMQ/Python_SDK/tree/v1.0.0/examples) 
are standalone projects that showcase the usage of the SDK.

To run the examples you need to have a running instance of KubeMQ.

The following example available under the `/examples` dir:

- `command-query-channel.py` - Initiate a Command Query on a channel
- `command-query-initator.py` - Initiate a Command Query
- `command-query-responder.py` - Respond to Command Queries 
- `event-channel.py` - Send event to KubeMQ on a channel
- `event-sender.py` - Send event to KubeMQ
- `event-sender-stream.py` - Stream event to KubeMQ
- `event-subscriber.py` - Subscribe to KubeMQ events

## Built With

* [Protocol Buffers](https://developers.google.com/protocol-buffers/) - Serializing structured data.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details
