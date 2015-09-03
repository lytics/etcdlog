package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/etcdlog"
)

func main() {
	host := flag.String("host", "http://localhost:2379", "URL to etcd")
	path := flag.String("path", "/", "path to watch (recursively)")
	index := flag.Uint64("index", 0, "index to start from")

	flag.Parse()

	c, err := etcdlog.NewEtcdClient([]string{*host})
	if err != nil {
		log.Fatal("error connecting to etcd: ", err)
	}

	// Output encoder
	enc := json.NewEncoder(os.Stdout)

	// Signal handler
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, os.Kill)

	// etcd watcher
	w := etcdlog.NewWatcher(c, *path, *index)
	resps := w.Watch()

mainloop:
	for {
		select {
		case resp, ok := <-resps:
			if !ok {
				break mainloop
			}
			if err := enc.Encode(resp); err != nil {
				log.Fatal("error encoding response: ", err)
			}
		case <-sigs:
			w.Close()
		}
	}

	if err := w.Err(); err != nil {
		if ee, ok := err.(*etcd.EtcdError); ok {
			log.Println(ee.Index)
		}
		log.Fatal("error when closing: ", err)
	}
}
