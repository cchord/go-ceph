package elector

import (
	"errors"
	"time"

	pb "github.com/cchord/go-ceph/pkg/mon/elector/electorpb"
	"github.com/golang/glog"
)

var ErrKeyNotFound = errors.New("requested key not found")

type Elector struct {
	whoami int64
	epoch  int64

	electingMe bool
	startStamp time.Time
	ackedMe    map[int64]struct{}

	storage KVStorage
	peers   map[int64]struct{}
	quorum  map[int64]struct{}

	recvc  chan pb.Message
	readyc chan []pb.Message

	msgs []pb.Message

	electionTimer   *time.Timer
	electionTimerC  <-chan time.Time
	electionTimeout time.Duration

	win  func()
	lose func()
}

func New(storage KVStorage, whoami int64, peers []int64, timeout time.Duration) *Elector {
	el := &Elector{
		whoami:          whoami,
		storage:         storage,
		peers:           make(map[int64]struct{}),
		recvc:           make(chan pb.Message),
		readyc:          make(chan []pb.Message),
		msgs:            make([]pb.Message, 0),
		electionTimeout: timeout,
	}

	for _, p := range peers {
		el.peers[p] = struct{}{}
	}

	var err error
	el.epoch, err = el.storage.GetInt64([]byte(`mon_epoch`))
	if err != nil {
		if err != ErrKeyNotFound {
			panic(err)
		}
		el.epoch = 0
	}

	if el.epoch == 0 {
		el.bumpEpoch(1) // electing cycle
	}

	go el.run()

	return el
}

func (el *Elector) run() {
	for {
		select {
		case m := <-el.recvc:
			el.step(m)
		case el.readyc <- el.msgs:
			el.msgs = make([]pb.Message, 0)
		case <-el.electionTimerC:
			el.expire()
		}
	}
}

func (el *Elector) step(m pb.Message) {
	switch m.Type {
	case pb.MsgElect:
		el.startElection()
	case pb.MsgRequestVote:
		el.handleRequestVote(m)
	case pb.MsgRequestVoteAck:
		el.handleRequestVoteAck(m)
	case pb.MsgVictory:
		el.handleVictory(m)
	default:
		glog.Warningln(el.whoami, el.epoch, "- unknown message received:", m.String())
	}
}

func (el *Elector) bumpEpoch(e int64) error {
	glog.V(10).Infoln(el.whoami, el.epoch, "- bumpEpoch", el.epoch, "to", e)
	assert(el.epoch < e)
	el.epoch = e
	err := el.storage.PutInt64([]byte(`mon_epoch`), el.epoch)
	if err != nil {
		glog.Errorln(el.whoami, el.epoch, " - failed to store mon_epoch:", err)
		return err
	}

	return nil
}

func (el *Elector) startElection() {
	glog.V(5).Infoln(el.whoami, el.epoch, "- start -- can i be leader?")

	if el.epoch%2 == 0 {
		el.bumpEpoch(el.epoch + 1)
	} else {
		el.bumpEpoch(el.epoch + 2)
	}

	glog.V(5).Infoln(el.whoami, el.epoch, "- bumping epoch to", el.epoch)

	el.startStamp = time.Now()
	el.electingMe = true
	el.ackedMe = make(map[int64]struct{})
	el.ackedMe[el.whoami] = struct{}{}

	for peer := range el.peers {
		if peer != el.whoami {
			el.msgs = append(el.msgs,
				pb.Message{
					Type:  pb.MsgRequestVote,
					From:  el.whoami,
					To:    peer,
					Epoch: el.epoch,
				})
		}
	}

	glog.V(5).Infoln(el.whoami, el.epoch, "- reseting timer")
	el.resetTimer()
}

func (el *Elector) winElection() {
	glog.V(5).Infoln(el.whoami, el.epoch, "- i win election!")

	el.quorum = el.ackedMe
	el.reset()

	assert(el.epoch%2 == 1)
	el.bumpEpoch(el.epoch + 1)

	for peer := range el.quorum {
		if peer != el.whoami {
			el.send(pb.Message{
				Type:  pb.MsgVictory,
				From:  el.whoami,
				To:    peer,
				Epoch: el.epoch,
			})
		}
	}

	// TODO: call some callbacks
	if el.win != nil {
		glog.V(5).Infoln(el.whoami, el.epoch, "- call win()")
		el.win()
	}
}

func (el *Elector) handleRequestVote(m pb.Message) {
	glog.V(5).Infoln(el.whoami, el.epoch, "- handle request vote message --", m.String())

	if m.Epoch <= el.epoch {
		glog.V(5).Infoln(el.whoami, el.epoch, "- epoch LE mine, reject --", m.String())
		// send a reject message with my epoch, let requester update its epoch.
		el.send(pb.Message{
			Type:    pb.MsgRequestVoteAck,
			From:    el.whoami,
			To:      m.From,
			Epoch:   el.epoch,
			Granted: false,
		})

		return
	}

	assert(m.Epoch%2 == 1)

	if el.epoch%2 == 0 {
		glog.V(5).Infoln(el.whoami, el.epoch, "- i am in non election cycle, received request vote message")
	}

	err := el.bumpEpoch(m.Epoch)
	if err != nil {
		glog.Errorln(el.whoami, el.epoch, "- failed to bump epoch: ", err)
		return
	}

	if el.whoami < m.From {
		glog.V(5).Infoln(el.whoami, el.epoch, "- i should be the leader!")
		// i should win!
		if !el.electingMe {
			el.startElection()
		}
		// TODO: send a reject message?
	} else {
		glog.V(5).Infoln(el.whoami, el.epoch, "- grant vote to", m.From)
		if el.electingMe {
			el.reset()
		}
		// grant vote
		el.send(pb.Message{
			Type:    pb.MsgRequestVoteAck,
			From:    el.whoami,
			To:      m.From,
			Epoch:   el.epoch,
			Granted: true,
		})
	}
}

func (el *Elector) handleRequestVoteAck(m pb.Message) {
	glog.V(5).Infoln(el.whoami, el.epoch, "- handle request vote ack message --", m.String())
	if m.Epoch < el.epoch {
		glog.V(5).Infoln(el.whoami, el.epoch, "- saw old epoch, drop stale request vote ack message:", m.String())
		return
	}

	if !m.Granted {
		assert(m.Epoch >= el.epoch)
		if m.Epoch > el.epoch {
			el.bumpEpoch(m.Epoch)
		}
		glog.V(5).Infoln(el.whoami, el.epoch, "- rejected by peer:", m.String())
		//el.startElection()
		return
	}

	if el.electingMe {
		el.ackedMe[m.From] = struct{}{}
		glog.V(5).Infoln(el.whoami, el.epoch, "- so far i have", el.ackedMe)
		if len(el.ackedMe) > len(el.peers)/2 {
			glog.V(5).Infoln(el.whoami, el.epoch, "- short cut to election finish")
			el.winElection()
		}
	} else {
		glog.V(5).Infoln(el.whoami, el.epoch, "- already quited election, stale request vote ack:", m.String())
	}
}

func (el *Elector) handleVictory(m pb.Message) {
	glog.V(5).Infoln(el.whoami, el.epoch, "- handle victory message --", m.String())
	if m.Epoch < el.epoch {
		glog.V(5).Infoln(el.whoami, el.epoch, "- saw old epoch, drop stale victory message:", m.String())
		return
	}

	assert(m.Epoch%2 == 0)
	assert(m.From < el.whoami)

	// TODO: call some callbacks
	if el.lose != nil {
		el.lose()
	}

	glog.V(5).Infoln(el.whoami, el.epoch, "-", m.From, "wins the election")

	if el.electingMe {
		el.reset()
	}
}

func (el *Elector) reset() {
	el.electingMe = false
	el.ackedMe = nil
	el.cancelTimer()
}

func (el *Elector) cancelTimer() {
	if el.electionTimer != nil {
		el.electionTimer.Stop()
		el.electionTimer = nil
		el.electionTimerC = nil
	}
}

func (el *Elector) resetTimer() {
	if el.electionTimer != nil {
		el.electionTimer.Stop()
	}
	el.electionTimer = time.NewTimer(el.electionTimeout)
	el.electionTimerC = el.electionTimer.C
}

func (el *Elector) send(m pb.Message) { el.msgs = append(el.msgs, m) }

func (el *Elector) expire() {
	glog.V(5).Infoln(el.whoami, el.epoch, "- election timer expired")

	if el.electingMe && len(el.ackedMe) > len(el.peers)/2 {
		el.winElection()
	} else {
		glog.V(5).Infoln(el.whoami, el.epoch, "- expired, i didn't get enough votes, retry")
		el.startElection()
	}
}

func (el *Elector) StartElection() {
	el.recvc <- pb.Message{Type: pb.MsgElect}
}

func (el *Elector) Step(m pb.Message) {
	el.recvc <- m
}
