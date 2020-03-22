package mpx

// TODO: think about concurrency

import (
	"github.com/RedisMPX/go-mpx/internal/list"
)

// A ChannelSubscription ties a OnMessageFunc to zero or more Redis Pub/Sub channels through
// a single multiplexed connection. Use NewChannelSubscription from Multiplexer to create a
// new ChannelSubscription.
// Before disposing of a ChannelSubscription you must call Close.
// ChannelSubscription instances are not safe for concurrent use.
type ChannelSubscription struct {
	mpx              *Multiplexer
	channels         map[string]*list.Element
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
		mpx,
		make(map[string]*list.Element),
		onMessage,
		onDisconnect,
		onActivation,
		node,
		false,
	}
	return node
}

// Adds a new Redis Pub/Sub channel to the ChannelSubscription.
func (s *ChannelSubscription) Add(chans ...string) {
	if s.closed {
		panic("tried to use a closed subscription")
	}
	for _, ch := range chans {
		_, ok := s.channels[ch]
		if !ok {
			node := list.NewElement(s)
			s.mpx.reqCh <- request{subscriptionAdd, ch, node}
			s.channels[ch] = node
		}
	}
}

// TODO: have remove return something to notify the user that the request failed?

// Removes a previously added Redis Pub/Sub channel from the ChannelSubscription.
func (s *ChannelSubscription) Remove(chans ...string) {
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

// Checks if a given Redis Pub/Sub channel is part of this ChannelSubscription.
// Complexity is O(1).
func (s ChannelSubscription) HasChannel(ch string) bool {
	if s.closed {
		panic("tried to use a closed subscription")
	}
	_, ok := s.channels[ch]
	return ok
}

// Returns a list of all Redis Pub/Sub channels present in the ChannelSubscription.
// The list is computed on-demand.
func (s ChannelSubscription) GetChannels() []string {
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

// Removes all Redis Pub/Sub channels from the ChannelSubscription.
func (s *ChannelSubscription) Clear() {
	if s.closed {
		panic("tried to use a closed ChannelSubscription")
	}
	for ch := range s.channels {
		s.Remove(ch)
	}

	// Reset our internal state
	s.channels = make(map[string]*list.Element)
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
