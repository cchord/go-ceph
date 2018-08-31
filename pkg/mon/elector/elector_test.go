package elector

import (
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/cchord/go-ceph/pkg/mon/elector/electorpb"
	"github.com/golang/glog"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type inMemStore struct {
	m map[string]interface{}
}

func (s *inMemStore) Get(key []byte) ([]byte, error) {
	panic("not implemented")
}

func (s *inMemStore) Put(key []byte, value []byte) error {
	panic("not implemented")
}

func (s *inMemStore) PutInt64(key []byte, value int64) error {
	s.m[string(key)] = value
	return nil
}

func (s *inMemStore) GetInt64(key []byte) (int64, error) {
	v, ok := s.m[string(key)]
	if !ok {
		return 0, ErrKeyNotFound
	}
	return v.(int64), nil
}

type aint64 int64

func (ai *aint64) Get() int64  { return atomic.LoadInt64((*int64)(ai)) }
func (ai *aint64) Set(v int64) { atomic.StoreInt64((*int64)(ai), v) }

type config struct {
	peers []int64
}

type cluster struct {
	electors map[int64]*Elector
	outc     map[int64]<-chan []pb.Message

	blocked map[int64]*aint64
}

func randTimeout(d time.Duration) time.Duration {
	return d + time.Duration(float64(d)*rand.Float64())
}

func newCluster(cfg config) *cluster {
	c := &cluster{
		make(map[int64]*Elector),
		make(map[int64]<-chan []pb.Message),
		make(map[int64]*aint64),
	}
	for _, peer := range cfg.peers {
		c.electors[peer] = New(&inMemStore{make(map[string]interface{})}, peer, cfg.peers, randTimeout(time.Millisecond*500))
		c.outc[peer] = c.electors[peer].readyc
		c.blocked[peer] = new(aint64)
	}

	go c.run()
	return c
}

func (ms *cluster) run() {
	for id := range ms.electors {
		go func(id int64) {
			for msgs := range ms.outc[id] {
				if ms.blocked[id].Get() == int64(1) {
					continue
				}
				for _, m := range msgs {
					time.Sleep(randTimeout(time.Millisecond * 50))
					if ms.blocked[m.To].Get() != int64(1) {
						ms.electors[m.To].Step(m)
					}
				}
			}
		}(id)
	}
}

func (ms *cluster) block(id int64)   { ms.blocked[id].Set(1) }
func (ms *cluster) unblock(id int64) { ms.blocked[id].Set(0) }

func TestNew(t *testing.T) {
	el := New(&inMemStore{make(map[string]interface{})}, 0, []int64{0}, time.Second)
	var w bool
	done := make(chan struct{})
	el.win = func() {
		w = true
		close(done)
	}
	el.StartElection()
	<-done
	if !w {
		t.Error("i should win election")
	}
}

func TestBasic(t *testing.T) {
	c := newCluster(config{[]int64{1, 2, 3, 4, 5}})
	countc := make(chan struct{})
	done := make(chan struct{})

	for _, el := range c.electors {
		el.win = func() {
			countc <- struct{}{}
		}
	}

	check := func(timeout time.Duration, maxleaders int) {
		n := 0
		for {
			select {
			case <-countc:
				n++
				if n > maxleaders {
					t.Error("leader election not stable...")
					done <- struct{}{}
					return
				}
			case <-time.After(timeout):
				glog.Infoln("during election,i saw", n, "leader(s)...")
				if n == 0 {
					t.Error("no leader elected...")
				}
				done <- struct{}{}
				return
			}
		}
	}

	{
		glog.Infoln("\n1 starts election...\n")
		c.electors[1].StartElection()
		go check(time.Second*5, 10)
		<-done
	}

	{
		glog.Infoln("\n2 starts election...\n")
		c.electors[2].StartElection()
		go check(time.Second*5, 10)
		<-done
	}

	{
		glog.Infoln("\n3 starts election...\n")
		c.electors[3].StartElection()
		go check(time.Second*15, 10)
		<-done
	}

	{
		glog.Infoln("\nall start election...\n")
		for id := range c.electors {
			go func(id int64) {
				c.electors[id].StartElection()
			}(id)
		}
		go check(time.Second*15, 10)
		<-done
	}

	{
		glog.Infoln("\nblock 1, 5 starts election...\n")
		c.block(1)
		c.electors[5].StartElection()
		go check(time.Second*15, 10)
		<-done
	}

	{
		glog.Infoln("\nblock 1, 5, all start election...\n")
		c.block(5)
		for id := range c.electors {
			go func(id int64) {
				c.electors[id].StartElection()
			}(id)
		}
		go check(time.Second*15, 10)
		<-done
	}
}
