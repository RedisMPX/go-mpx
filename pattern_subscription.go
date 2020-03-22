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
	mpx              *Multiplexer
	pattern          string
	onMessage        OnMessageFunc
	onDisconnect     OnDisconnectFunc
	onActivation     OnActivationFunc
	onDisconnectNode *list.Element
	onMessageNode    *list.Element
	closed           bool
}

func createPatternSubscription(
	mpx *Multiplexer,
	pattern string,
	onMessage OnMessageFunc,
	onDisconnect OnDisconnectFunc,
	onActivation OnActivationFunc,
) *list.Element {
	onDisconnectNode := list.NewElement(nil)
	patSub := PatternSubscription{
		mpx,
		pattern,
		onMessage,
		onDisconnect,
		onActivation,
		onDisconnectNode,
		nil,
		false,
	}
	patSub.onMessageNode = list.NewElement(&patSub)
	onDisconnectNode.Value = &patSub
	return onDisconnectNode
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
	p.mpx.reqCh <- request{patternClose, "", p.onDisconnectNode}
}
