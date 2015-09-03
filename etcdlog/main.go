package main

import (
	"encoding/json"
	"flag"
	"io"
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
	jsonout := flag.String("json", "-", "file to write json output to; '-' for stdout, '' for nowhere")
	nojson := flag.Bool("nojson", false, "same as -json=''")
	humout := flag.String("log", "", "file to write human output to; '-' for stderr, '' for nowhere")

	flag.Parse()

	if *nojson {
		*jsonout = ""
	}

	if *jsonout == "" && *humout == "" {
		log.Fatal("expected one of -json or -log")
	}

	c, err := etcdlog.NewEtcdClient([]string{*host})
	if err != nil {
		log.Fatal("error connecting to etcd: ", err)
	}

	// JSON output encoder
	var enc *json.Encoder
	switch *jsonout {
	case "-":
		enc = json.NewEncoder(os.Stdout)
	case "":
		// don't even set it
	default:
		jfd, err := os.Create(*jsonout)
		if err != nil {
			log.Fatalf("error creating json output file %s: %v", *jsonout, err)
		}
		defer jfd.Close()
		enc = json.NewEncoder(jfd)
	}

	// Human output encoder
	var hlog io.Writer
	switch *humout {
	case "-":
		hlog = os.Stderr
	case "":
		// don't even set it
	default:
		hfd, err := os.Create(*humout)
		if err != nil {
			log.Fatalf("error creating log output file %s: %v", *humout, err)
		}
		defer hfd.Close()
		hlog = hfd
	}

	// Signal handler
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, os.Kill)

	// etcd watcher
	w := etcdlog.NewWatcher(c, *path, *index)
	resps := w.Watch()

	newline := []byte("\n")

mainloop:
	for {
		select {
		case resp, ok := <-resps:
			if !ok {
				break mainloop
			}
			// Write human readable log if writer exists
			if hlog != nil {
				if _, err := hlog.Write([]byte(resp.String())); err != nil {
					log.Fatal("error writing log output: ", err)
				}
				hlog.Write(newline)
			}

			// Write JSON if encoder exists
			if enc != nil {
				if err := enc.Encode(resp); err != nil {
					log.Fatal("error encoding response: ", err)
				}
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
