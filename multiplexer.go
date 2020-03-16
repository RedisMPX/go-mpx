// Package mpx implements a Redis Pub/Sub multiplexer.
package mpx

import (
	"fmt"
	"github.com/RedisMPX/go-mpx/internal/list"
	"github.com/RedisMPX/go-mpx/types"
	"github.com/gomodule/redigo/redis"
	"time"
)

type request struct {
	isUnsub bool
	ch      string
	elem    *list.Element
}

// A multiplexer instance corresponds to one Redis Pub/Sub connection.
// A single multiplexer can create multiple subscriptions that will reuse
// the same underlying connection. Use mpx.New() to create a new Multiplexer.
// The multiplexer is safe for concurrent use.
type Multiplexer struct {
	pubsub    redis.PubSubConn
	listeners map[string]*list.List
	reqCh     chan request
}

// Creates a new Multiplexer. The input function must provide a new connection
// whenever called. The Multiplexer will only use it to create a new connection
// in case of errors, meaning that a Multiplexer will only have one active connection
// to Redis at a time.
func New(createConn func() redis.Conn) Multiplexer {
	r := createConn()
	mpx := multiplexer{
		pubsub:    redis.PubSubConn{Conn: r},
		listeners: make(map[string]*list.List),
		reqCh:     make(chan request),
	}

	go mpx.run()

	return mpx

}

func (mpx *Multiplexer) NewSubscription(fn types.ListenerFunc) Subscription {
	return createSubscription(mpx, fn)
}

func (mpx *Multiplexer) run() {
	messageCh := mpx.initChannel(100)

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
						panic(err)
					}
				}

			} else {
				// ADD
				if !ok {
					listeners = list.New()
					mpx.listeners[req.ch] = listeners
					// Subscribe in Redis
					if err := mpx.pubsub.Subscribe(req.ch); err != nil {
						panic(err)
					}
				}

				// Store the listener function
				listeners.AssimilateElement(req.elem)
			}
		// READ FROM REDIS PUB/SUB
		case msg, ok := <-messageCh:
			if !ok {
				// We are leaving because the underlying client has been closed
				// which means we are not needed anymore for sure.
				return
			}
			l, _ := mpx.listeners[msg.Channel]

			for e := l.Front(); e != nil; e = e.Next() {
				e.Value(msg.Channel, msg.Data)
			}

		}
	}
}

func (mpx *Multiplexer) initChannel(size int) chan redis.Message {
	const timeout = 30 * time.Second

	ch := make(chan redis.Message, size)
	ping := make(chan struct{}, 1)

	go func() {
		timer := time.NewTimer(timeout)
		timer.Stop()

		var errCount int
		for {
			// Any message is as good as a ping.
			select {
			case ping <- struct{}{}:
			default:
			}

			errCount = 0
			switch msg := mpx.pubsub.Receive().(type) {
			case redis.Subscription:
				// Ignore.
			case redis.Pong:
				// Ignore.
			case redis.Message:
				timer.Reset(timeout)
				select {
				case ch <- msg:
					if !timer.Stop() {
						<-timer.C
					}
				case <-timer.C:
					// drop message
				}
			case error:
				fmt.Printf("error in Pubsub %v\n", errCount)
				// if err == pool.ErrClosed {
				// 	close(ch)
				// 	return
				// }
				if errCount > 0 {
					// time.Sleep(mpx.pubsub.retryBackoff(errCount))
				}
				errCount++
			default:

			}
		}
	}()

	go func() {
		timer := time.NewTimer(timeout)
		timer.Stop()

		healthy := true
		for {
			timer.Reset(timeout)
			select {
			case <-ping:
				healthy = true
				if !timer.Stop() {
					<-timer.C
				}
			case <-timer.C:
				_ = mpx.pubsub.Ping("")
				if healthy {
					healthy = false
				} else {
					// if pingErr == nil {
					// 	pingErr = errPingTimeout
					// }
					// mpx.pubsub.mu.Lock()
					// mpx.pubsub._reconnect(pingErr)
					// mpx.pubsub.mu.Unlock()
				}
				// case <-mpx.pubsub.exit:
				// 	return
			}
		}
	}()

	return ch
}
