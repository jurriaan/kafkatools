# Kafka Tools - small cli tools for monitoring and managing [Apache Kafka](http://kafka.apache.org/) [![Build Status](https://travis-ci.org/jurriaan/kafkatools.svg?branch=master)](https://travis-ci.org/jurriaan/kafkatools)

## Installation

```bash
$ go get github.com/jurriaan/kafkatools/...
```

## consumer_offsets

CLI tool for fetching kafka consumer group offsets and lag with support for inserting the results into InfluxDB

## reset_consumer_group

Simple CLI tool for resetting the offset of a kafka consumer group

## License

Copyright 2016 Jurriaan Pruis

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
