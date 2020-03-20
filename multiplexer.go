// Package mpx implements a Redis Pub/Sub multiplexer.
package mpx

import (
	"errors"
	"fmt"
	"github.com/RedisMPX/go-mpx/internal"
	"github.com/RedisMPX/go-mpx/internal/list"
	"github.com/gomodule/redigo/redis"
	"strings"
	"time"
)

var errPingTimeout = errors.New("redis: ping timeout")

// A function that Subscriber will trigger every time a new message
// is recevied on a Redis Pub/Sub channel that was added to it.
type OnMessageFunc = func(channel string, message []byte)

// A function that Subscriber will trigger every time the Redis Pub/Sub
// connection has been lost. The error argument will be the error that
// triggered the reconnection event.
// Subscriber ensures that this function will be triggered *after* all
// pending messages have been delivered.
type OnDisconnectFunc = func(error)

// A function that Subscriber will trigger once a connection has been
// re-established. Subscriber ensures that this function will be triggered
// *before* new messages start coming from the new connection.
type OnReconnectFunc = func()

type requestType int

const (
	subscriptionAdd requestType = iota
	subscriptionRemove
	subscriptionClose
)

type request struct {
	reqType requestType
	ch      string
	elem    *list.Element
}

// A Multiplexer instance corresponds to one Redis Pub/Sub connection that
// will be shared by multiple Subscription instances.
// A Multiplexer must be created with New.
// Multiplexer instances are safe for concurrent use.
type Multiplexer struct {
	// Options
	pingTimeout time.Duration
	minBackOff  time.Duration
	maxBackOff  time.Duration

	// state
	createConn      func() (redis.Conn, error)
	pubsub          redis.PubSubConn
	listeners       map[string]*list.List
	reqCh           chan request
	exit            chan struct{}
	messages        chan interface{}
	mustReconnectCh chan error
	subscriptions   *list.List
}

// Creates a new Multiplexer. The input function must provide a new connection
// whenever called. The Multiplexer will only use it to create a new connection
// in case of errors, meaning that a Multiplexer will only have one active connection
// to Redis at a time. Multiplexers will automatically try to reconnect using
// an exponential backoff (plus jitter) algorithm.
func New(createConn func() (redis.Conn, error)) *Multiplexer {
	size := 1000

	mpx := Multiplexer{
		pingTimeout:     5 * time.Second,
		minBackOff:      8 * time.Millisecond,
		maxBackOff:      512 * time.Millisecond,
		pubsub:          redis.PubSubConn{Conn: nil},
		createConn:      createConn,
		listeners:       make(map[string]*list.List),
		reqCh:           make(chan request, 100),
		exit:            make(chan struct{}),
		messages:        make(chan interface{}, size),
		mustReconnectCh: make(chan error),
		subscriptions:   list.New(),
	}

	mpx.openNewConnection()

	go mpx.reconnect()
	go mpx.startSending()
	go mpx.startReading(100)

	return &mpx

}

// Creates a new Subscription tied to the Multiplexer. Subscription instances
// must be closed (see the relative Close method) before being disposed of.
// Subscription instances are not safe for concurrent use.
func (mpx *Multiplexer) NewSubscription(onMessage OnMessageFunc, onDisconnect OnDisconnectFunc, onReconnect OnReconnectFunc) Subscription {
	if onMessage == nil {
		panic("onMessage cannot be nil")
	}
	subscriptionNode := createSubscription(mpx, onMessage, onDisconnect, onReconnect)
	mpx.subscriptions.AssimilateElement(subscriptionNode)
	return subscriptionNode.Value.(Subscription)
}

func (mpx *Multiplexer) Close() {

}

// Ensures only one reconnection event happens at a time.
// Goroutines should return *immediately* after calling this function.
func (mpx *Multiplexer) triggerReconnect(err error) {
	// Try to trigger a reconnection.
	select {
	case mpx.mustReconnectCh <- err:
	default:
	}

	// Drain the message that indicates the caller goroutine is exiting.
	<-mpx.exit
}

func (mpx *Multiplexer) openNewConnection() {
	// Open a new connection
	var c redis.Conn
	var errorCount int
	for {
		var err error
		c, err = mpx.createConn()
		if err == nil {
			// No error, got a good connection, we move forward.
			break
		}
		if errorCount > 0 {
			time.Sleep(internal.RetryBackoff(errorCount, mpx.minBackOff, mpx.maxBackOff))
		}
		errorCount++
	}

	mpx.pubsub.Conn = c
}

func (mpx *Multiplexer) reconnect() {
	for {
		// Wait for somebody to trigger a reconnection event.
		err := <-mpx.mustReconnectCh
		fmt.Printf("Reconnect triggered, error: [%v]\n", err)

		// Close the old connection
		if err := mpx.pubsub.Close(); err != nil {
			// TODO: once we have some kind of logging,
			//       maybe write a line about this minor failure.
		}

		// Kill all goroutines
		mpx.exit <- struct{}{}
		mpx.exit <- struct{}{}

		// Start an "emergency" goroutine that keeps processing
		// requests without doing any I/O while we try to establish
		// a new connection.
		go mpx.offlineProcessing(err)

		// Open a new connection.
		mpx.openNewConnection()

		// Stop the offline processing goroutine
		mpx.exit <- struct{}{}

		// Gather all Redis Pub/Sub channel names in a slice
		chList := make([]interface{}, len(mpx.listeners))

		var idx int
		for ch := range mpx.listeners {
			chList[idx] = ch
			idx += 1
		}

		// Resubscribe to all the Redis Pub/Sub channels
		if len(chList) > 0 {
			mpx.pubsub.Subscribe(chList...)
		}

		// Restart the goroutines
		go mpx.startSending()
		go mpx.startReading(100)
	}
}

// This goroutine is kinda like the emergency command hologram of this lib
func (mpx *Multiplexer) offlineProcessing(err error) {
	// Drain the mpx.messages channel.
	close(mpx.messages)
	for msg := range mpx.messages {
		mpx.dispatchMessage(msg)
	}
	mpx.messages = make(chan interface{}, 100)

	// Notify Subscriptions of the disconnection
	for e := mpx.subscriptions.Front(); e != nil; e = e.Next() {
		onDisconnect := e.Value.(Subscription).onDisconnect

		if onDisconnect != nil {
			onDisconnect(err)
		}
	}

	// Keep processing requests until we're notified
	// that a connecton has been re-established.
	for {
		select {
		case req := <-mpx.reqCh:
			mpx.processRequest(req, false)
		case <-mpx.exit:
			// Notify Subscriptions that we reconnected
			for e := mpx.subscriptions.Front(); e != nil; e = e.Next() {
				onReconnect := e.Value.(Subscription).onReconnect

				if onReconnect != nil {
					onReconnect()
				}
			}
			return
		}
	}
}

func (mpx *Multiplexer) processRequest(req request, networkIO bool) error {
	switch req.reqType {
	case subscriptionClose:
		mpx.subscriptions.Remove(req.elem)
	case subscriptionAdd:
		listeners, ok := mpx.listeners[req.ch]

		if !ok {
			listeners = list.New()
			mpx.listeners[req.ch] = listeners
		}

		// Store the listener function
		listeners.AssimilateElement(req.elem)

		// Leaving the Networked I/O for last so that failures
		// don't leave us in an inconsistent state.
		if !ok {
			if networkIO {
				// Subscribe in Redis
				if err := mpx.pubsub.Subscribe(req.ch); err != nil {
					return err
				}
			}
		}
	case subscriptionRemove:
		listeners, ok := mpx.listeners[req.ch]

		if !ok {
			return nil
		}

		listeners.Remove(req.elem)

		if listeners.Len() > 0 {
			fmt.Printf("[ws] unsubbed but more remaining (%v)\n", listeners.Len())
		} else {
			fmt.Printf("[ws] unsubbed also from Redis\n")
			delete(mpx.listeners, req.ch)
			if networkIO {
				if err := mpx.pubsub.Unsubscribe(req.ch); err != nil {
					return err
				}
			}
		}

	}
	return nil
}

func (mpx *Multiplexer) dispatchMessage(msgInterface interface{}) {
	switch msg := msgInterface.(type) {
	case redis.Pong:
		// We need to receive pongs even if we discard them
		// because they reset the activityTimer and prevent
		// a reconnection event from happening.
	case redis.Message:
		l, ok := mpx.listeners[msg.Channel]
		if ok {
			for e := l.Front(); e != nil; e = e.Next() {
				e.Value.(OnMessageFunc)(msg.Channel, msg.Data)
			}
		}
	case redis.Subscription:
		// l, ok := mpx.listeners[msg.Channel]
		// if ok {
		// 	for e := l.Front(); e != nil; e = e.Next() {
		// 		e.Value(msg.Channel, msg.Data)
		// 	}
		// }
	}
}

// TODO: WE NEED PIPELINING FOR SUBSCRIPTIONS and UNSUBSCRIPTIONS
func (mpx *Multiplexer) startSending() {
	healthy := true
	activityTimer := time.NewTimer(mpx.pingTimeout)

	for {
		select {
		case req := <-mpx.reqCh:
			if err := mpx.processRequest(req, true); err != nil {
				mpx.triggerReconnect(err)
				return
			}
		// READ FROM REDIS PUB/SUB
		case msg := <-mpx.messages:
			// We just received a message so we stop the counter that
			// makes us produce PING messages.
			healthy = true
			if !activityTimer.Stop() {
				<-activityTimer.C
			}

			// Dispatch the message we just received.
			mpx.dispatchMessage(msg)

			// Now that we dispatched the message, we restart the timer.
			activityTimer.Reset(mpx.pingTimeout)

		case <-activityTimer.C:
			// The timer has triggered, we need to ping Redis.
			activityTimer.Reset(mpx.pingTimeout)

			pingErr := mpx.pubsub.Conn.Send("PING")
			if pingErr == nil {
				pingErr = mpx.pubsub.Conn.Flush()
			}
			if pingErr != nil {
				mpx.triggerReconnect(pingErr)
				return
			}

			if healthy {
				healthy = false
			} else {
				if pingErr == nil {
					pingErr = errPingTimeout
				}

				mpx.triggerReconnect(pingErr)
				return
			}
		case <-mpx.exit:
			return
		}
	}
}

func (mpx *Multiplexer) startReading(size int) {
	for {
		switch msg := mpx.pubsub.Receive().(type) {
		case error:
			// This is a dirty hack to ignore ping replies
			// coming from when the connection is not yet in
			// pubsub mode.
			// TODO: ask redigo do accept pings also from the pubsub
			//       interface.
			if !strings.HasSuffix(msg.Error(), "got type string") {
				fmt.Printf("error in Pubsub: %v", msg)
				mpx.triggerReconnect(msg)
				return
			}
			mpx.messages <- redis.Pong{}
		default:
			mpx.messages <- msg
		}
	}
}
