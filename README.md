# distributed-metadata-index
CS2270 Final Project

Current version is a simple implementation: Linear search each key-value pair in etcd.

## etcd cluster setup

`Note: This only works on Mac.`

Install [etcd](https://github.com/etcd-io/etcd/releases/).

Install [Goreman](https://github.com/mattn/goreman). On Mac, run `brew install goreman` is a shortcut.

Refer to [etcd-cluster](https://etcd.io/docs/v3.4/dev-guide/local_cluster/) to set up a local multi-member cluster. For the `Procfile`, refer to [Procfile](https://github.com/etcd-io/etcd/blob/main/Procfile).