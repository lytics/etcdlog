package etcdlog

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

const recursive = true

type Response struct {
	WatchIndex uint64
	Timestamp  time.Time `json:"timestamp"`
	*etcd.Response
}

func (r *Response) String() string {
	ts := r.Timestamp.Format("2006-01-02 15:04:05.999999999")
	if len(r.Node.Nodes) > 0 {
		return fmt.Sprintf("%-29s %d->%d %-16s %s %s %d %d (%d)", ts, r.WatchIndex, r.EtcdIndex, r.Action, r.Node.Key, r.Node.Value, r.Node.CreatedIndex, r.Node.ModifiedIndex, len(r.Node.Nodes))
	} else {
		return fmt.Sprintf("%-29s %d->%d %-16s %s %s %d %d", ts, r.WatchIndex, r.EtcdIndex, r.Action, r.Node.Key, r.Node.Value, r.Node.CreatedIndex, r.Node.ModifiedIndex)
	}
}

type Watcher struct {
	c     *etcd.Client
	path  string
	index uint64
	err   error

	stopped int64
	stop    chan bool
}

func NewWatcher(c *etcd.Client, path string, index uint64) *Watcher {
	return &Watcher{c: c, path: path, stop: make(chan bool)}
}

// Watch returns a buffered chan of etcd responses. When the chan is closed,
// callers should check Err() to get the reason.
//
// Watch always starts watching the index the Watcher was created at.
func (w *Watcher) Watch() <-chan *Response {
	out := make(chan *Response, 100)
	index := w.index
	go func() {
		defer close(out)

		if index == 0 {
			resp, err := w.c.Get(w.path, false, false)
			if err != nil {
				w.err = err
				return
			}
			index = resp.EtcdIndex
		}

		for {
			// Start the blocking watch after the last response's index.
			rawResp, err := protectedRawWatch(w.c, w.path, index, recursive, nil, w.stop)
			now := time.Now() // grab wallclock as close to event as possible
			if err != nil {
				if err == etcd.ErrWatchStoppedByUser {
					// This isn't actually an error, the stop chan was closed. Time to stop!
					return
				}

				// This is probably a canceled request panic
				// Wait a little bit, then continue as normal
				// Can be removed after Go 1.5 is released
				if ispanic(err) {
					time.Sleep(250 * time.Millisecond)
					continue
				}

				// Other RawWatch errors should be retried forever. If the node refresher
				// also fails to communicate with etcd it will close the coordinator,
				// closing ec.stop in the process which will cause this function to with
				// ErrWatchStoppedByUser.
				transport.CloseIdleConnections() // paranoia; let's get fresh connections on errors.
				continue
			}

			if len(rawResp.Body) == 0 {
				// This is a bug in Go's HTTP + go-etcd + etcd which causes the
				// connection to timeout perdiocally and need to be restarted *after*
				// closing idle connections.
				transport.CloseIdleConnections()
				continue
			}

			resp, err := rawResp.Unmarshal()
			if err != nil {
				w.err = err
				return
			}
			select {
			case out <- &Response{index, now, resp}:
				// was index++ which could result in receiving an event more than once
				// for non-root paths
				index = resp.Node.ModifiedIndex + 1
			case <-w.stop:
				return
			}
		}
	}()
	return out
}

func (w *Watcher) Err() error { return w.err }

// Close the Watcher; closes the response chan returned by Watch(). Do not
// check Err() until Watch() chan is closed.
func (w *Watcher) Close() {
	if !atomic.CompareAndSwapInt64(&w.stopped, 0, 1) {
		return
	}
	close(w.stop)
}
