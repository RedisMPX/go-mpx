package mpx

// TODO: think about concurrency

import (
	"github.com/RedisMPX/go-mpx/internal/list"
)

// A Subscription ties a callback to zero or more Redis Pub/Sub
// channels through a single multiplexed connection.
// Use the NewSubscription() method from Multiplexer to create a new Subscription.
// Subscription instances are not safe for concurrent use.
type Subscription struct {
	mpx      *Multiplexer
	channels map[string]*list.Element
	fn       ListenerFunc
}

func createSubscription(mpx *Multiplexer, fn ListenerFunc) Subscription {
	return Subscription{mpx, make(map[string]*list.Element), fn}
}

// Adds a new Redis Pub/Sub channel to the Subscription.
func (s *Subscription) Add(chans ...string) {
	for _, ch := range chans {
		_, ok := s.channels[ch]
		if !ok {
			elem := list.NewElement(s.fn)
			s.mpx.reqCh <- request{false, ch, elem}
			s.channels[ch] = elem
		}
	}
}

// TODO: have remove return something to notify the user that the request failed?

// Removes a previously added Redis Pub/Sub channel from the Subscription.
func (s *Subscription) Remove(chans ...string) {
	for _, ch := range chans {
		elem, ok := s.channels[ch]
		if ok {
			s.mpx.reqCh <- request{true, ch, elem}
			delete(s.channels, ch)
		}
	}
}

// Checks if a given Redis Pub/Sub channel is part of this subscription.
// Complexity is O(1).
func (s Subscription) HasChannel(ch string) bool {
	_, ok := s.channels[ch]
	return ok
}

// Returns a list of all Redis Pub/Sub channels present in the Subscription.
// The list is computed on-demand.
func (s Subscription) GetChannels() []string {
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
	for ch := range s.channels {
		s.Remove(ch)
	}

	// Reset our internal state
	s.channels = make(map[string]*list.Element)
}
