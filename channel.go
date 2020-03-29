package mpx

// TODO: think about concurrency

import (
	"github.com/RedisMPX/go-mpx/internal/list"
)

// A ChannelSubscription ties a OnMessageFunc to zero or more Redis Pub/Sub channels through
// a single multiplexed connection. Use NewChannelSubscription from Multiplexer to create a
// new ChannelSubscription.
// Before disposing of a ChannelSubscription you must call Close on it.
//
// ChannelSubscription instances are not safe for concurrent use.
type ChannelSubscription struct {
	// Map that contains the Redis Pub/Sub channels
	// added to the subscription. Useful for testing
	// membership and obtaining a list of names.
	// Do not modify directly.
	Channels         map[string]*list.Element
	mpx              *Multiplexer
	onMessage        OnMessageFunc
	onDisconnect     OnDisconnectFunc
	onActivation     OnActivationFunc
	onDisconnectNode *list.Element
	closed           bool
}

func createChannelSubscription(
	mpx *Multiplexer,
	onMessage OnMessageFunc,
	onDisconnect OnDisconnectFunc,
	onActivation OnActivationFunc,
) *list.Element {
	node := list.NewElement(nil)
	node.Value = &ChannelSubscription{
		make(map[string]*list.Element),
		mpx,
		onMessage,
		onDisconnect,
		onActivation,
		node,
		false,
	}
	return node
}

// Adds a new Redis Pub/Sub channel to the ChannelSubscription.
func (s *ChannelSubscription) Add(channels ...string) {
	if s.closed {
		panic("tried to use a closed subscription")
	}
	for _, ch := range channels {
		_, ok := s.Channels[ch]
		if !ok {
			node := list.NewElement(s)
			s.mpx.reqCh <- request{subscriptionAdd, ch, node}
			s.Channels[ch] = node
		}
	}
}

// TODO: have remove return something to notify the user that the request failed?

// Removes a previously added Redis Pub/Sub channel from the ChannelSubscription.
func (s *ChannelSubscription) Remove(channels ...string) {
	if s.closed {
		panic("tried to use a closed subscription")
	}
	for _, ch := range channels {
		node, ok := s.Channels[ch]
		if ok {
			s.mpx.reqCh <- request{subscriptionRemove, ch, node}
			delete(s.Channels, ch)
		}
	}
}

// Removes all Redis Pub/Sub channels from the ChannelSubscription.
func (s *ChannelSubscription) Clear() {
	if s.closed {
		panic("tried to use a closed ChannelSubscription")
	}
	for ch := range s.Channels {
		s.Remove(ch)
	}

	// Reset our internal state
	s.Channels = make(map[string]*list.Element)
}

// Calls Clear and frees all references from the Multiplexer.
// After calling this method the ChannelSubscription instance
// cannot not be used any more. There is no need to call Close
// if you are also disposing of the whole Multiplexer.
func (s *ChannelSubscription) Close() {
	if s.closed {
		panic("tried to use a closed ChannelSubscription")
	}

	s.Clear()
	s.closed = true
	s.mpx.reqCh <- request{subscriptionClose, "", s.onDisconnectNode}
}
