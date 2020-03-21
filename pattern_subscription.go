package mpx

import (
	"github.com/RedisMPX/go-mpx/internal/list"
)

// A PatternSubscription ties a OnMessageFunc to one Redis Pub/Sub pattern through
// a single multiplexed connection. Use NewPatternSubscription from Multiplexer to create a
// new PatternSubscription. PatternSubscription instances are not safe for concurrent use.
// Before disposing of a PatternSubscription you must call Close.
// PatternSubscription instances should not be copied.
// For more information about pattern syntax: https://redis.io/topics/pubsub#pattern-matching-subscriptions
type PatternSubscription struct {
	mpx           *Multiplexer
	pattern       string
	onMessage     OnMessageFunc
	onDisconnect  OnDisconnectFunc
	onReconnect   OnReconnectFunc
	onNotifNode   *list.Element
	onMessageNode *list.Element
	closed        bool
}

func createPatternSubscription(
	mpx *Multiplexer,
	pattern string,
	onMessage OnMessageFunc,
	onDisconnect OnDisconnectFunc,
	onReconnect OnReconnectFunc,
) *list.Element {
	onNotifNode := list.NewElement(nil)

	onNotifNode.Value = &PatternSubscription{
		mpx,
		pattern,
		onMessage,
		onDisconnect,
		onReconnect,
		onNotifNode,
		list.NewElement(onMessage),
		false,
	}
	return onNotifNode
}

// Returns the pattern that this PatternSubscription is subscribed to.
func (p PatternSubscription) GetPattern() string {
	return p.pattern
}

// Closes the PatternSubscription and frees all allocated resources.
func (p *PatternSubscription) Close() {
	if p.closed {
		panic("tried to use a closed PatternSubscription")
	}
	p.closed = true
	p.mpx.reqCh <- request{patternClose, "", p.onNotifNode}
}
