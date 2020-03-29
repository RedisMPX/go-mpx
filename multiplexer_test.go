package mpx

import (
	"github.com/gomodule/redigo/redis"
	"testing"
	"time"
)

func TestMultiplexer(t *testing.T) {
	connBuilder := func() (redis.Conn, error) {
		return redis.Dial("tcp", ":6379")
	}

	conn, err := connBuilder()
	if err != nil {
		panic(err)
	}

	multiplexer := New(connBuilder)

	messages := make(chan []byte, 100)
	onMessage := func(_ string, msg []byte) {
		messages <- msg
	}

	errors := make(chan error, 100)
	onDisconnect := func(err error) {
		errors <- err
	}

	activations := make(chan string, 100)
	onActivation := func(ch string) {
		activations <- ch
	}

	sub := multiplexer.NewChannelSubscription(onMessage, onDisconnect, onActivation)

	// Activation works
	{
		// Add a Redis Pub/Sub channel to the Subscription
		sub.Add("mychannel")
		timer := time.NewTimer(3 * time.Second)
		select {
		case <-activations:
			break
		case <-timer.C:
			t.Errorf("timed out while waiting for the 1st activation")
		}
	}

	// Able to reconnect
	{
		_, err := conn.Do("client", "kill", "type", "pubsub")
		if err != nil {
			panic(err)
		}

		timer := time.NewTimer(3 * time.Second)

		select {
		case <-errors:
			break
		case <-timer.C:
			t.Errorf("timed out while waiting for the error notification")
		}

		select {
		case <-activations:
			break
		case <-timer.C:
			t.Errorf("timed out while waiting for the 2nd activation")
		}
	}

}
