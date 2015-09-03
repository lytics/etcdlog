etcdlog
=======

```sh
go get github.com/lytics/etcdlog/etcdlog

etcdlog    # works if you have etcd running locally
etcdlog -h # for help

# JSON output (human readable output by default)
etcdlog -log="" -json="-"

# Human readable logging w/json to a file
etcdlog -json="etcdlog.json" -log="-"
```

Some code borrowed from https://github.com/lytics/metafora
