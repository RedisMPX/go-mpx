// Package mpx implements a Redis Pub/Sub multiplexer.
package mpx

import (
	"errors"
	"fmt"
	"github.com/RedisMPX/go-mpx/internal"
	"github.com/RedisMPX/go-mpx/internal/list"
	"github.com/gomodule/redigo/redis"
	"time"
)

var errPingTimeout = errors.New("redis: ping timeout")

// A function that Subscriber will trigger every time a new message
// is recevied on a Redis Pub/Sub channel that was added to it.
type ListenerFunc = func(channel string, message []byte)

type request struct {
	isUnsub bool
	ch      string
	elem    *list.Element
}

// A Multiplexer instance corresponds to one Redis Pub/Sub connection.
// A single Multiplexer can create multiple subscriptions that will reuse
// the same underlying connection. Use mpx.New() to create a new Multiplexer.
// The Multiplexer is safe for concurrent use.
type Multiplexer struct {
	pingTimeout time.Duration
	minBackOff  time.Duration
	maxBackOff  time.Duration

	createConn      func() (redis.Conn, error)
	pubsub          redis.PubSubConn
	listeners       map[string]*list.List
	reqCh           chan request
	exit            chan struct{}
	messages        chan redis.Message
	mustReconnectCh chan error
}

// Creates a new Multiplexer. The input function must provide a new connection
// whenever called. The Multiplexer will only use it to create a new connection
// in case of errors, meaning that a Multiplexer will only have one active connection
// to Redis at a time.
func New(createConn func() (redis.Conn, error)) Multiplexer {
	size := 1000
	// TODO: make this right
	c, _ := createConn()
	mpx := Multiplexer{
		pingTimeout:     5 * time.Second,
		minBackOff:      8 * time.Millisecond,
		maxBackOff:      512 * time.Millisecond,
		pubsub:          redis.PubSubConn{Conn: c},
		createConn:      createConn,
		listeners:       make(map[string]*list.List),
		reqCh:           make(chan request, 100),
		exit:            make(chan struct{}),
		messages:        make(chan redis.Message, size),
		mustReconnectCh: make(chan error),
	}

	go mpx.reconnect()
	go mpx.startSending()
	go mpx.startReading(100)

	return mpx

}

// Creates a new Subcription which will call the provided callback for every
// message sent to it. See the documentation for Subscription to learn how to
// add and remove channels to/from it.
// Subscription instances are not safe for concurrent use.
func (mpx *Multiplexer) NewSubscription(fn ListenerFunc) Subscription {
	return createSubscription(mpx, fn)
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

		// TODO MAYBE Ensure the msg channel is drained

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

		mpx.pubsub = redis.PubSubConn{Conn: c}

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

// TODO: WE NEED PIPELINING FOR SUBSCRIPTIONS and UNSUBSCRIPTIONS
func (mpx *Multiplexer) startSending() {
	healthy := true
	activityTimer := time.NewTimer(mpx.pingTimeout)

	for {
		select {
		case req := <-mpx.reqCh:
			listeners, ok := mpx.listeners[req.ch]

			if req.isUnsub {
				// REMOVE
				if !ok {
					continue
				}

				listeners.Remove(req.elem)

				if listeners.Len() > 0 {
					fmt.Printf("[ws] unsubbed but more remaining (%v)\n", listeners.Len())
				} else {
					fmt.Printf("[ws] unsubbed also from Redis\n")
					delete(mpx.listeners, req.ch)
					if err := mpx.pubsub.Unsubscribe(req.ch); err != nil {
						mpx.triggerReconnect(err)
						return
					}
				}

			} else {
				// ADD
				if !ok {
					listeners = list.New()
					mpx.listeners[req.ch] = listeners
					// Subscribe in Redis
					if err := mpx.pubsub.Subscribe(req.ch); err != nil {
						mpx.triggerReconnect(err)
						return
					}
				}

				// Store the listener function
				listeners.AssimilateElement(req.elem)
			}
		// READ FROM REDIS PUB/SUB
		case msg := <-mpx.messages:
			// We just received a message so we stop the counter that
			// makes us produce PING messages.
			healthy = true
			if !activityTimer.Stop() {
				<-activityTimer.C
			}

			l, ok := mpx.listeners[msg.Channel]
			if ok {
				for e := l.Front(); e != nil; e = e.Next() {
					e.Value(msg.Channel, msg.Data)
				}
			}

			// Now that we dispatched the message, we restart the timer.
			activityTimer.Reset(mpx.pingTimeout)

		case <-activityTimer.C:
			// The timer has triggered, we need to ping Redis.
			activityTimer.Reset(mpx.pingTimeout)

			if len(mpx.listeners) == 0 {
				_, syncPingErr := mpx.pubsub.Conn.Do("PING")
				if syncPingErr != nil {
					mpx.triggerReconnect(syncPingErr)
					return
				}
				healthy = true
				continue
			}

			pingErr := mpx.pubsub.Ping("")
			if healthy {
				healthy = false
			} else {
				if pingErr == nil {
					pingErr = errPingTimeout
				}

				mpx.triggerReconnect(pingErr)
				return
			}
		default:
			// TODO: test is this is really giving higher priority to draining the msg channel
			// we want this to avoid the case where a leftover msg gets delivered like 30s later
			select {
			case <-mpx.exit:
				return
			default:
			}
		}
	}
}

func (mpx *Multiplexer) startReading(size int) {
	const timeout = 30 * time.Second

	dropMsgTimer := time.NewTimer(timeout)
	if !dropMsgTimer.Stop() {
		<-dropMsgTimer.C
	}

	for {
		select {
		case <-mpx.exit:
			return
		default:
		}

		switch msg := mpx.pubsub.Receive().(type) {
		case redis.Subscription:
			// Ignore.
			mpx.messages <- redis.Message{"x", "x", []byte("x")}
		case redis.Pong:
			// Ignore.
			mpx.messages <- redis.Message{"x", "x", []byte("x")}
		case redis.Message:
			dropMsgTimer.Reset(timeout)
			select {
			case mpx.messages <- msg:
				if !dropMsgTimer.Stop() {
					<-dropMsgTimer.C
				}
			case <-dropMsgTimer.C:
				// drop message
			}
		case error:
			fmt.Printf("error in Pubsub: %v", msg)
			mpx.triggerReconnect(msg)
			return
		default:
			panic("???")
		}
	}

}
