# distributed-metadata-index

CS2270 Final Project

Current version is a simple implementation: Linear search each key-value pair in etcd.

## Setup

### etcd cluster setup

`Note: This only works on Mac.`

Install [etcd](https://github.com/etcd-io/etcd/releases/).

Install [Goreman](https://github.com/mattn/goreman). On Mac, run `brew install goreman` is a shortcut.

Refer to [etcd-cluster](https://etcd.io/docs/v3.4/dev-guide/local_cluster/) to set up a local multi-member cluster. For the `Procfile`, refer to [Procfile](https://github.com/etcd-io/etcd/blob/main/Procfile).

### zookeeper ensemble setup

Install [docker](https://docs.docker.com/get-docker/)

Run `docker-compose -f zk_stack.yml up`

### distributed-metadata-index command-line install

```
cd ./cmd/dmi
go build
```

## Features

### Regular Expression Searches

The metadata to be indexed is a set of key-value pairs. The key, which we called a **tag** is in the form of `"tag_name=tag_value”`. The search on tag_name supports 2 kinds of wildcards: **\*-wildcard** and **?-wildcard**.

1. \*-wildcard: matches zero or more characters
2. ?-wildcard: matches any single character

For example:

```
Storage: ["abcdefgh", "abcfkh", "abfh", "abfffh", "abfaah"]
Search String: "ab*f?h"
Result: [["abcfkh", "abcdefgh", "abfffh"]]
```

For more examples, see testcase [TestAdvancedWildcard](https://github.com/Zhe-Shen/distributed-metadata-index/blob/2022e4394bd1e8db7fc2d810d3371c8e8b1bdb93/test/zk_test.go#L77)
