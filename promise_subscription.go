package mpx

import (
	"github.com/RedisMPX/go-mpx/internal/list"
	"sync"
	"time"
)

// A PromiseSubscription allows you to wait for individual Redis Pub/Sub messages
// whith support for timeouts. This effectively creates a networked promise system.
// It makes use of a PatternSubscription internally to make creating new promises
// as lightweight as possible (no subscrube/unsubscribe command is sent to Redis
// to fullfill or expire a Promise). Unlike other types of subscriptions,
// PromiseSubscriptions are safe for concurrent use.
type PromiseSubscription struct {
	patSub   *PatternSubscription
	prefix   string
	channels map[string]*list.List
	reqCh    chan promiseRequest
	exit     chan struct{}
	closed   bool
	mu       sync.Mutex
}

// TODO: think about special characters in the prefix (eg "bana*")
func createPromiseSubscription(mpx *Multiplexer, prefix string) (*list.Element, *PromiseSubscription) {
	proSub := &PromiseSubscription{
		patSub:   nil,
		prefix:   prefix,
		channels: make(map[string]*list.List),
		reqCh:    make(chan promiseRequest, 100),
		exit:     make(chan struct{}),
		closed:   false,
		mu:       sync.Mutex{},
	}

	patSubNode := createPatternSubscription(mpx,
		prefix+"*",
		proSub.onMessage,
		proSub.onDisconnect,
		proSub.onReconnect,
	)
	proSub.patSub = patSubNode.Value.(*PatternSubscription)

	go proSub.messageDispatchGoroutine()
	return patSubNode, proSub
}

// A Promise represents a timed, uninterrupted, single-message
// subscription to a Redis Pub/Sub channel.
// If network connectivity gets lost, thus causing an interruption,
// the Promise will be failed (unless already fullfilled).
// Use NewPromise from PromiseSubscription to create a new Promise.
type Promise struct {
	// Possible outcomes:
	// - promise fulliflled: one message will be sent, then the channel will be closed
	// - promise timed out: channel will be closed
	// - promise canceled: channel will be closed
	// - network error: channel will be closed
	C          <-chan []byte
	closeableC chan []byte
	pSub       *PromiseSubscription
	node       *list.Element
	timer      *time.Timer
}

// Cancels a Promise.
func (promise *Promise) Cancel() {
	promise.pSub.mu.Lock()
	defer promise.pSub.mu.Unlock()

	promise.timer.Stop()

	if l := promise.node.DetachFromList(); l != nil {
		close(promise.closeableC)
	}
}

// Creates a new Promise for the given suffix.
func (p *PromiseSubscription) NewPromise(suffix string, timeout time.Duration) *Promise {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		panic("tried to use a alread closed PromiseSubscription")
	}

	channel := p.prefix + suffix
	promiseNode := list.NewElement(nil)

	normalGolangChannel := make(chan []byte, 1)
	promise := &Promise{
		C:          normalGolangChannel,
		closeableC: normalGolangChannel,
		pSub:       p,
		node:       promiseNode,
		timer:      nil,
	}
	promiseNode.Value = promise

	listeners, ok := p.channels[channel]
	if !ok {
		listeners = list.New()
		p.channels[channel] = listeners
	}

	listeners.AssimilateElement(promiseNode)

	// Set the timeout.
	// BTW this runs inside a single goroutine inside Go's runtime.
	// See https://golang.org/src/runtime/time.go runOneTimer() (around line 759)
	// Maybe it's a bad idea to bog down the timer code with this lock?
	// Maybe it's not?
	promise.timer = time.AfterFunc(timeout, promise.Cancel)
	return promise
}

func (p *PromiseSubscription) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		panic("tried to use a alread closed PromiseSubscription")
	}

	p.closed = true
	p.exit <- struct{}{}
	p.patSub.Close()
	<-p.exit
}

type promiseRequest struct {
	isError bool
	channel string
	message []byte
}

// These functions will be called by the main Multiplexer goroutine.
// For this reason we want to keep them as lightweight as possible,
// so we do the O(N) work in our own goroutine.
func (p *PromiseSubscription) onMessage(channel string, message []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.reqCh <- promiseRequest{false, channel, message}
	}
}
func (p *PromiseSubscription) onDisconnect(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.reqCh <- promiseRequest{true, "", []byte{}}
	}
}

func (p *PromiseSubscription) onReconnect() {
	// TODO: what do I do?
}

func (p *PromiseSubscription) messageDispatchGoroutine() {
	for {
		select {
		case <-p.exit:
			for _, l := range p.channels {
				for e := l.Front(); e != nil; e = e.Next() {
					promise := e.Value.(*Promise)
					close(promise.closeableC)
					promise.timer.Stop()
					e.Disassemble()
				}
			}
			close(p.exit)
			return
		case req := <-p.reqCh:
			p.mu.Lock()
			l, ok := p.channels[req.channel]
			if ok {
				for e := l.Front(); e != nil; e = e.Next() {
					promise := e.Value.(*Promise)
					if !req.isError {
						promise.closeableC <- req.message
					}
					close(promise.closeableC)
					promise.timer.Stop()
					e.Disassemble()
				}
				delete(p.channels, req.channel)
			}
			p.mu.Unlock()
		}
	}
}
