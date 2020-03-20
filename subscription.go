package mpx

// TODO: think about concurrency

import (
	"github.com/RedisMPX/go-mpx/internal/list"
)

// A Subscription ties a ListenerFunc to zero or more Redis Pub/Sub channels through
// a single multiplexed connection. Use NewSubscription from Multiplexer to create a
// new Subscription. Subscription instances are not safe for concurrent use.
// Before disposing of a Subscription you must call Close.
type Subscription struct {
	mpx             *Multiplexer
	channels        map[string]*list.Element
	onMessage       OnMessageFunc
	onDisconnect    OnDisconnectFunc
	onReconnect     OnReconnectFunc
	multiplexerNode *list.Element
	closed          bool
}

func createSubscription(mpx *Multiplexer, onMessage OnMessageFunc, onDisconnect OnDisconnectFunc, onReconnect OnReconnectFunc) *list.Element {
	node := list.NewElement(nil)
	node.Value = Subscription{
		mpx,
		make(map[string]*list.Element),
		onMessage,
		onDisconnect,
		onReconnect,
		node,
		false,
	}
	return node
}

// Adds a new Redis Pub/Sub channel to the Subscription.
func (s *Subscription) Add(chans ...string) {
	if s.closed {
		panic("tried to use a closed subscription")
	}
	for _, ch := range chans {
		_, ok := s.channels[ch]
		if !ok {
			node := list.NewElement(s.onMessage)
			s.mpx.reqCh <- request{subscriptionAdd, ch, node}
			s.channels[ch] = node
		}
	}
}

// TODO: have remove return something to notify the user that the request failed?

// Removes a previously added Redis Pub/Sub channel from the Subscription.
func (s *Subscription) Remove(chans ...string) {
	if s.closed {
		panic("tried to use a closed subscription")
	}
	for _, ch := range chans {
		node, ok := s.channels[ch]
		if ok {
			s.mpx.reqCh <- request{subscriptionRemove, ch, node}
			delete(s.channels, ch)
		}
	}
}

// Checks if a given Redis Pub/Sub channel is part of this subscription.
// Complexity is O(1).
func (s Subscription) HasChannel(ch string) bool {
	if s.closed {
		panic("tried to use a closed subscription")
	}
	_, ok := s.channels[ch]
	return ok
}

// Returns a list of all Redis Pub/Sub channels present in the Subscription.
// The list is computed on-demand.
func (s Subscription) GetChannels() []string {
	if s.closed {
		panic("tried to use a closed subscription")
	}
	chList := make([]string, len(s.channels))

	var idx int
	for ch := range s.channels {
		chList[idx] = ch
		idx += 1
	}

	return chList
}

// Removes all Redis Pub/Sub channels from the Subscription.
func (s *Subscription) Clear() {
	if s.closed {
		panic("tried to use a closed subscription")
	}
	for ch := range s.channels {
		s.Remove(ch)
	}

	// Reset our internal state
	s.channels = make(map[string]*list.Element)
}

// Calls Clear and frees all associated resources. After calling this method
// the Subcription instance cannot not be used any more.
func (s *Subscription) Close() {
	if s.closed {
		panic("tried to use a closed subscription")
	}

	s.Clear()
	s.closed = true
	s.mpx.reqCh <- request{subscriptionClose, "", s.multiplexerNode}
}
