package mpx

import (
	"errors"
	"github.com/RedisMPX/go-mpx/internal/list"
	"sync"
	"time"
)

// A PromiseSubscription allows you to wait for individual Redis Pub/Sub messages
// with support for timeouts. This effectively creates a networked promise system.
// It makes use of a PatternSubscription internally to make creating new promises
// as lightweight as possible (no subscrube/unsubscribe command is sent to Redis
// to fullfill or expire a Promise). Unlike other types of subscriptions,
// PromiseSubscriptions *are* safe for concurrent use
// Consider always calling WaitForActivation after creating a new PromiseSubscription.
type PromiseSubscription struct {
	patSub         *PatternSubscription
	prefix         string
	channels       map[string]*list.List
	reqCh          chan promiseRequest
	exit           chan struct{}
	isPatSubActive bool
	activationCond *sync.Cond
	closed         bool
	mu             sync.Mutex
}

// TODO: think about special characters in the prefix (eg "bana*")
func createPromiseSubscription(mpx *Multiplexer, prefix string) (*list.Element, *PromiseSubscription) {
	proSub := &PromiseSubscription{
		patSub:         nil,
		prefix:         prefix,
		channels:       make(map[string]*list.List),
		reqCh:          make(chan promiseRequest, 100),
		exit:           make(chan struct{}),
		isPatSubActive: false,
		activationCond: nil,
		closed:         false,
		mu:             sync.Mutex{},
	}
	proSub.activationCond = sync.NewCond(&proSub.mu)

	patSubNode := createPatternSubscription(mpx,
		prefix+"*",
		proSub.onMessage,
		proSub.onDisconnect,
		proSub.onActivation,
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
	done       bool
}

// Cancels a Promise. Calling Cancel more than once,
// or on an already-fullfilled Promise is a noop.
func (p *Promise) Cancel() {
	// TODO make done an atomic check before grabbing the lock
	// sync.Once doesn't seem to fit the bill because it forces
	// us to choose one single function, while we have two slightly
	// different procedures depending whether we are failing a single
	// promise vs failing all of them at once because of a disconnection.
	p.pSub.mu.Lock()
	defer p.pSub.mu.Unlock()
	if p.done {
		return
	}

	p.timer.Stop()

	if l := p.node.DetachFromList(); l != nil {
		close(p.closeableC)
	}
}

// Error returned by NewPromise when the subscription is not active.
// See WaitForActivation and WaitForNewPromise for alternative solutions.
var InactiveSubscriptionError = errors.New("the subscription is currently inactive")

// When a PromiseSubscription is first created (and after a disconnection event)
// it is not immediately able to create new Promises because it first needs to
// wait for the underlying PatternSubscription to become active. This function
// will block the caller until such condition is fullfilled.
// All waiters will be also unlocked when the subscription gets closed, so it's
// important to check for the return value before attempting to use it.
//
//  if sub.WaitForActivation() {
//     // make use of the subscription
//  }
//
func (p *PromiseSubscription) WaitForActivation() (ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.isPatSubActive {
		return
	}

	p.activationCond.Wait()
	return p.closed
}

// Like NewPromise, but it will wait for the PromiseSubscription to become
// active instead of returning an error. The timeout will start only *after*
// the function returns. All waiters will also be unlocked if the subscription
// gets closed, so it's important to check the second return value before
// attempting to use the returned Promise. Closing the subscription is
// the only way of making this function fail.
//
//  if promise, ok := sub.WaitForNewPromise(pfx, t_out); ok {
//     // make use of the promise
//  }
//
//
func (p *PromiseSubscription) WaitForNewPromise(suffix string, timeout time.Duration) (*Promise, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if p.closed {
			return nil, false
		}

		res, err := p.locklessNewPromise(suffix, timeout)
		if err == nil {
			return res, true
		}

		p.activationCond.Wait()
	}
}

// Creates a new Promise for the given suffix. The suffix gets composed with the
// prefix specified when creating the PromiseSubscription to create the final
// Redis Pub/Sub channel name. The underlying PatternSubscription will
// receive all messages sent under the given prefix, thus ensuring that new
// promises get into effect as soon as this method returns.
// Trying to create a new Promise while the PromiseSubscription is not active
// will cause this method to return InactiveSubscriptionError.
func (p *PromiseSubscription) NewPromise(suffix string, timeout time.Duration) (*Promise, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		panic("tried to use a alread closed PromiseSubscription")
	}
	return p.locklessNewPromise(suffix, timeout)
}

func (p *PromiseSubscription) locklessNewPromise(suffix string, timeout time.Duration) (*Promise, error) {
	if !p.isPatSubActive {
		return nil, InactiveSubscriptionError
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
	return promise, nil
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

	p.activationCond.Broadcast()
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

	p.isPatSubActive = false

	if !p.closed {
		p.reqCh <- promiseRequest{true, "", []byte{}}
	}
}

func (p *PromiseSubscription) onActivation(name string) {
	// TODO: don't really need the lock, could do CAS,
	// since Broadcast doesn't need the lock
	p.mu.Lock()
	defer p.mu.Unlock()
	p.isPatSubActive = true
	// Notify all goroutines waiting for the subscription to become
	// active that we're now back in business.
	p.activationCond.Broadcast()
}

func (p *PromiseSubscription) messageDispatchGoroutine() {
	for {
		select {
		case <-p.exit:
			p.fullfillOrFail(promiseRequest{true, "", []byte{}})
			close(p.exit)
			return
		case req := <-p.reqCh:
			p.fullfillOrFail(req)
		}
	}
}

func (p *PromiseSubscription) fullfillOrFail(req promiseRequest) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !req.isError {
		// Got a message, fullfill the relative promises
		l, ok := p.channels[req.channel]
		if ok {
			for e := l.Front(); e != nil; e = e.Next() {
				promise := e.Value.(*Promise)
				promise.closeableC <- req.message
				close(promise.closeableC)
				promise.timer.Stop()
				e.Disassemble()
			}
			delete(p.channels, req.channel)
		}
	} else {
		// Got a disconnection, invalidate all promises
		for _, l := range p.channels {
			for e := l.Front(); e != nil; e = e.Next() {
				promise := e.Value.(*Promise)
				close(promise.closeableC)
				promise.timer.Stop()
				// TODO: is it really a good idea to disassemble
				// all pointers? does it really make it easier for
				// the garbage collector? what do the comments in
				// the original implementation of list mean?
				e.Disassemble()
			}
		}
		p.channels = make(map[string]*list.List)
	}
}
