// Package mpx implements a Redis Pub/Sub multiplexer.
package mpx

import (
	"errors"
	"fmt"
	"github.com/RedisMPX/go-mpx/internal"
	"github.com/RedisMPX/go-mpx/internal/list"
	"github.com/gomodule/redigo/redis"
	"strings"
	"sync"
	"time"
)

var errPingTimeout = errors.New("redis: ping timeout")

// A function that gets triggered whenever a message is received on
// a given Redis Pub/Sub channel.
type OnMessageFunc = func(channel string, message []byte)

// A function that gets triggered every time the Redis Pub/Sub
// connection has been lost. The error argument will be the error that
// caused the reconnection event.
// This function will be triggered *after* all pending messages have been dispatched.
type OnDisconnectFunc = func(error)

// A function that gets triggered whenever a subscription goes into effect.
//  - Subscription: name is a Redis Pub/Sub channel
//  - PatternSubscription: name is a Redis Pub/Sub pattern
type OnActivationFunc = func(name string)

type requestType int

const (
	subscriptionInit requestType = iota
	subscriptionAdd
	subscriptionRemove
	subscriptionClose
	patternInit
	patternClose
)

type request struct {
	reqType requestType
	name    string
	node    *list.Element
}

// A Multiplexer instance corresponds to one Redis Pub/Sub connection that
// will be shared by multiple subscription instances.
// A Multiplexer must be created with New.
// Multiplexer instances are safe for concurrent use.
type Multiplexer struct {
	// Options
	pingTimeout time.Duration
	minBackOff  time.Duration
	maxBackOff  time.Duration

	// state
	createConn       func() (redis.Conn, error)
	pubsub           redis.PubSubConn
	channels         map[string]*list.List
	patterns         map[string]*list.List
	reqCh            chan request
	stop             chan struct{}
	exit             chan struct{}
	stopWG           sync.WaitGroup
	readerGoroExitWG sync.WaitGroup
	messages         chan interface{}
	mustReconnectCh  chan error
	subscriptions    *list.List
}

// Creates a new Multiplexer. The input function must provide a new connection
// whenever called. The Multiplexer will only use it to create a new connection
// in case of errors, meaning that a Multiplexer will only have one active connection
// to Redis at a time. Multiplexers will automatically try to reconnect using
// an exponential backoff (plus jitter) algorithm.
func New(createConn func() (redis.Conn, error)) *Multiplexer {
	// TODO: how do we notify the user that we can't make a connection immediately?

	size := 1000

	mpx := Multiplexer{
		pingTimeout:      30 * time.Second,
		minBackOff:       8 * time.Millisecond,
		maxBackOff:       512 * time.Millisecond,
		pubsub:           redis.PubSubConn{Conn: nil},
		createConn:       createConn,
		channels:         make(map[string]*list.List),
		patterns:         make(map[string]*list.List),
		reqCh:            make(chan request, 100),
		stop:             make(chan struct{}),
		exit:             make(chan struct{}),
		stopWG:           sync.WaitGroup{},
		readerGoroExitWG: sync.WaitGroup{},
		messages:         make(chan interface{}, size),
		mustReconnectCh:  make(chan error),
		subscriptions:    list.New(),
	}

	mpx.stopWG.Add(1)
	go mpx.connectionGoroutine()
	return &mpx
}

// Creates a new Subscription tied to the Multiplexer. Before disposing of a Subcription you
// must call Close (see the relative Close method for extra advice).
// Subscription instances are not safe for concurrent use.
// The arguments onDisconnect and onReconnect can be nil if you're not interested in the
// corresponding type of events. All event listeners will be called sequentially from
// a single goroutine. Depending on the workload, consider keeping all functions lean
// and offload slow operations to other goroutines if necessary.
func (mpx *Multiplexer) NewChannelSubscription(
	onMessage OnMessageFunc,
	onDisconnect OnDisconnectFunc,
	onActivation OnActivationFunc,
) *ChannelSubscription {
	if onMessage == nil {
		panic("onMessage cannot be nil")
	}
	subscriptionNode := createSubscription(mpx, onMessage, onDisconnect, onActivation)
	mpx.reqCh <- request{subscriptionInit, "", subscriptionNode}
	return subscriptionNode.Value.(*ChannelSubscription)
}

// Creates a new PatternSubcription tied to the Multiplexer. Before disposing of a PatternSubcription you
// must call its Close method.
// The arguments onDisconnect and onActivation can be nil if you're not interested in the
// corresponding types of event. All event listeners will be called sequentially from
// a single goroutine. Depending on the workload, consider keeping all functions lean
// and offload slow operations to other goroutines whenever possible.
// PatternSubscription instances are not safe for concurrent use.
// For more information about pattern syntax: https://redis.io/topics/pubsub#pattern-matching-subscriptions
func (mpx *Multiplexer) NewPatternSubscription(
	pattern string,
	onMessage OnMessageFunc,
	onDisconnect OnDisconnectFunc,
	onActivation OnActivationFunc,
) *PatternSubscription {
	// TODO: what about empty patterns?
	if onMessage == nil {
		panic("onMessage cannot be nil")
	}

	patternSubNode := createPatternSubscription(mpx, pattern, onMessage, onDisconnect, onActivation)
	mpx.reqCh <- request{patternInit, pattern, patternSubNode}
	return patternSubNode.Value.(*PatternSubscription)
}

// Creates a new PromiseSubscription. PromiseSubscriptions are safe for concurrent use.
// The prefix argument is used to create a PatternSubscription that will match all
// channels that start with the provided prefix.
func (mpx *Multiplexer) NewPromiseSubscription(prefix string) *PromiseSubscription {
	// TODO: what about empty patterns?

	patternSubNode, promiseSub := createPromiseSubscription(mpx, prefix)
	mpx.reqCh <- request{patternInit, promiseSub.patSub.pattern, patternSubNode}
	return promiseSub
}

// Restarts a stopped Multiplexer. Calling Restart on a Multiplexer that was not
// stopped will trigger a panic.
func (mpx *Multiplexer) Restart() {
	select {
	case <-mpx.stop:
		mpx.stop = make(chan struct{})
		mpx.messages = make(chan interface{}, 100)
		mpx.mustReconnectCh = make(chan error, 100)
		mpx.stopWG.Add(1)
		go mpx.connectionGoroutine()
	default:
		panic("called Restart non a Multiplexer that was not stopped")
	}
}

// Stops all service goroutines and closes the underlying Redis Pub/Sub connection.
// The Multiplexer will still be tied to the Subscriptions created from it, so
// it's the callers duty to avoid calling Add / Remove / Clear from those Subscriptions
// while the Multiplexer is stopped. Stopped Multiplexers can be restarted (see Restart).
func (mpx *Multiplexer) Stop() {
	select {
	case <-mpx.stop:
		panic("called Stop on a Multiplexer that was already stopped")
	default:
	}

	close(mpx.stop)
	mpx.stopWG.Wait()

	// Close the connection to make sure the reader goroutine exits.
	// We might be tryng to exit before having established the first
	// connection, so we need to check for it to be != nil.
	if mpx.pubsub.Conn != nil {
		if err := mpx.pubsub.Close(); err != nil {
			// TODO: once we have some kind of logging,
			//       maybe write a line about this minor failure.
		}
	}

	// We wait for the messageReaderGoroutine to stop (if it's running)
	mpx.readerGoroExitWG.Wait()

	// Drain the mpx.messages channel.
	// We also use this as a synchronization point with messageReaderGoroutine
	close(mpx.messages)
	for msg := range mpx.messages {
		mpx.dispatchMessage(msg)
	}
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
	// But always keep an eye on the stop channel.
	select {
	case <-mpx.exit:
	case <-mpx.stop:
	}
}

func (mpx *Multiplexer) openNewConnection() {
	// Open a new connection
	var c redis.Conn
	var errorCount int
	for {
		select {
		case <-mpx.stop:
			return
		default:
		}

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

func (mpx *Multiplexer) connectionGoroutine() {
	defer mpx.stopWG.Done()

	// The first time we enter the loop we are trying
	// to establish the connection for the first time.
	var err error
	for {

		if err != nil {
			// Close the old connection
			if err := mpx.pubsub.Close(); err != nil {
				// TODO: once we have some kind of logging,
				//       maybe write a line about this minor failure.
			}

			// Kill requestProcessingGoroutine and messageReadingGoroutine.
			// While we want to block to push to mpx.exit twice, we also
			// want to be able to exit if the Multiplexer was closed in the
			// meantime.
			for i := 0; i < 2; i += 1 {
				select {
				case mpx.exit <- struct{}{}:
				case <-mpx.stop:
					return
				}
			}
		}

		// Start an "emergency" goroutine that keeps processing
		// requests without doing any I/O while we try to establish
		// a new connection.
		mpx.stopWG.Add(1)
		go mpx.offlineProcessingGoroutine(err)

		// Open a new connection.
		mpx.openNewConnection()

		// Stop the offline processing goroutine
		select {
		case mpx.exit <- struct{}{}:
		case <-mpx.stop:
			return
		}

		// Gather all Redis Pub/Sub channel names in a slice
		{
			chList := make([]interface{}, len(mpx.channels))

			var idx int
			for ch := range mpx.channels {
				chList[idx] = ch
				idx += 1
			}

			// Resubscribe to all the Redis Pub/Sub channels
			if len(chList) > 0 {
				mpx.pubsub.Subscribe(chList...)
			}

		}

		// Gather all Redis Pub/Sub patterns in a slice
		{
			// TODO: can this be optimized, like reuse that was allocated before?
			patList := make([]interface{}, len(mpx.patterns))

			var idx int
			for ch := range mpx.patterns {
				patList[idx] = ch
				idx += 1
			}

			// Resubscribe to all the Redis Pub/Sub channels
			if len(patList) > 0 {
				mpx.pubsub.PSubscribe(patList...)
			}

		}
		// Restart the goroutines
		mpx.stopWG.Add(1)
		go mpx.requestProcessingGoroutine()
		mpx.readerGoroExitWG.Add(1)
		go mpx.messageReadingGoroutine(100)

		// Wait for somebody to trigger a reconnection event.
		// If the error is nil, it means that this is the first
		// connection attempt after instantiating the Multiplexer.
		select {
		case err = <-mpx.mustReconnectCh:
			select {
			case <-mpx.stop:
				return
			default:
			}
		case <-mpx.stop:
			return
		}
	}
}

// This goroutine is kinda like the emergency command hologram of this package.
func (mpx *Multiplexer) offlineProcessingGoroutine(err error) {
	defer mpx.stopWG.Done()

	// Drain the mpx.messages channel.
	close(mpx.messages)
	for msg := range mpx.messages {
		mpx.dispatchMessage(msg)
	}
	mpx.messages = make(chan interface{}, 100)

	if err != nil {
		// Notify Subscriptions of the disconnection
		for e := mpx.subscriptions.Front(); e != nil; e = e.Next() {

			var onDisconnect OnDisconnectFunc
			switch s := e.Value.(type) {
			case *ChannelSubscription:
				onDisconnect = s.onDisconnect
			case *PatternSubscription:
				onDisconnect = s.onDisconnect
			}

			if onDisconnect != nil {
				onDisconnect(err)
			}
		}
	}

	// Keep processing requests until we're notified
	// that a connecton has been re-established.
	for {
		select {
		case <-mpx.stop:
			return
		case req := <-mpx.reqCh:
			mpx.processRequest(req, false)
		case <-mpx.exit:
			return
		}
	}
}

func (mpx *Multiplexer) processRequest(req request, networkIO bool) error {
	switch req.reqType {
	case subscriptionInit:
		mpx.subscriptions.AssimilateElement(req.node)
	case subscriptionAdd:
		listeners, ok := mpx.channels[req.name]

		if !ok {
			listeners = list.New()
			mpx.channels[req.name] = listeners
		}

		// Store the listener function
		listeners.AssimilateElement(req.node)

		// Leaving the Networked I/O for last so that failures
		// don't leave us in an inconsistent state.
		if !ok {
			if networkIO {
				// Subscribe in Redis
				if err := mpx.pubsub.Subscribe(req.name); err != nil {
					return err
				}
			}
		} else {
			// We were already subscribed to that channel
			if networkIO {
				// If we are not in offline-mode, we immediately send confirmation
				// that the subscription is active. If we are in the case where
				// we just lost connectivity and processRequestGoroutine has not yet
				// noticed, we will send a wrong notification, but we will also soon
				// send a onDisconnect notification, after this goroutine exits, thus
				// rectifying our wrong communication.
				if onActivation := req.node.Value.(*ChannelSubscription).onActivation; onActivation != nil {
					onActivation(req.name)
				}
			}
		}
	case subscriptionRemove:
		if listeners := req.node.DetachFromList(); listeners != nil {
			if listeners.Len() > 0 {
				fmt.Printf("[ws] unsubbed but more remaining (%v)\n", listeners.Len())
			} else {
				fmt.Printf("[ws] unsubbed also from Redis\n")
				delete(mpx.channels, req.name)
				if networkIO {
					if err := mpx.pubsub.Unsubscribe(req.name); err != nil {
						return err
					}
				}
			}
		}
	case subscriptionClose:
		mpx.subscriptions.Remove(req.node)
	case patternInit:
		// Add node to connection status notifications
		mpx.subscriptions.AssimilateElement(req.node)

		// Add different node to onMessage notifications
		onMessageNode := req.node.Value.(*PatternSubscription).onMessageNode
		listeners, ok := mpx.patterns[req.name]

		if !ok {
			listeners = list.New()
			mpx.patterns[req.name] = listeners
		}

		// Store the listener function
		listeners.AssimilateElement(onMessageNode)

		// Leaving the Networked I/O for last so that failures
		// don't leave us in an inconsistent state.
		if !ok {
			if networkIO {
				// Subscribe in Redis
				if err := mpx.pubsub.PSubscribe(req.name); err != nil {
					return err
				}
			}
		} else {
			// We were already subscribed to that channel
			if networkIO {
				// If we are not in offline-mode, we immediately send confirmation
				// that the subscription is active. If we are in the case where
				// we just lost connectivity and processRequestGoroutine has not yet
				// noticed, we will send a wrong notification, but we will also soon
				// send a onDisconnect notification, after this goroutine exits, thus
				// rectifying our wrong communication.
				if onActivation := req.node.Value.(*PatternSubscription).onActivation; onActivation != nil {
					onActivation(req.name)
				}
			}
		}
	case patternClose:
		// Unsub from connection state notifications
		mpx.subscriptions.Remove(req.node)

		// Unsub from messages
		onMessageNode := req.node.Value.(*PatternSubscription).onMessageNode
		if listeners := onMessageNode.DetachFromList(); listeners != nil {
			if listeners.Len() > 0 {
				fmt.Printf("[ws] unsubbed but more remaining (%v)\n", listeners.Len())
			} else {
				fmt.Printf("[ws] unsubbed also from Redis\n")
				delete(mpx.patterns, req.name)
				if networkIO {
					if err := mpx.pubsub.PUnsubscribe(req.name); err != nil {
						return err
					}
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
		// Channels
		l, ok := mpx.channels[msg.Channel]
		if ok {
			for e := l.Front(); e != nil; e = e.Next() {
				e.Value.(*ChannelSubscription).onMessage(msg.Channel, msg.Data)
			}
		}

		// Patterns
		l, ok = mpx.patterns[msg.Pattern]
		if ok {
			for e := l.Front(); e != nil; e = e.Next() {
				e.Value.(*PatternSubscription).onMessage(msg.Channel, msg.Data)
			}
		}
	case redis.Subscription:
		switch msg.Kind {
		case "subscribe":
			// Channels
			l, ok := mpx.channels[msg.Channel]
			if ok {
				for e := l.Front(); e != nil; e = e.Next() {
					if onActivation := e.Value.(*ChannelSubscription).onActivation; onActivation != nil {
						onActivation(msg.Channel)
					}
				}
			}
		case "psubscribe":
			// Patterns
			l, ok := mpx.patterns[msg.Channel]
			if ok {
				for e := l.Front(); e != nil; e = e.Next() {
					if onActivation := e.Value.(*PatternSubscription).onActivation; onActivation != nil {
						onActivation(msg.Channel)
					}
				}
			}
		}
	}
}

// TODO: WE NEED PIPELINING FOR SUBSCRIPTIONS and UNSUBSCRIPTIONS
func (mpx *Multiplexer) requestProcessingGoroutine() {
	defer mpx.stopWG.Done()

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
		case <-mpx.stop:
			return
		}
	}
}

// Since we're forced to block with Receive() potentially forever,
// there is no good way for us to peek into mpx.exit.
// As a consequence, if an error was reported from another
// goroutine, the reconnect() procedure will try to close
// the old connection to make sure Receive() returns
// with an error, which will give us then an opporunity to exit.
func (mpx *Multiplexer) messageReadingGoroutine(size int) {
	defer mpx.readerGoroExitWG.Done()
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
			select {
			case mpx.messages <- redis.Pong{}:
			case <-mpx.stop:
				return
			}
		default:
			select {
			case mpx.messages <- msg:
			case <-mpx.stop:
				return
			}
		}
	}
}
