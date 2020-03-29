package mpx

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"testing"
	"time"
)

func TestPattern(t *testing.T) {
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

	sub := multiplexer.NewPatternSubscription("pattern:*", onMessage, onDisconnect, onActivation)

	// Activation works
	{
		timer := time.NewTimer(3 * time.Second)
		select {
		case <-activations:
			break
		case <-timer.C:
			t.Errorf("timed out while waiting for the 1st activation")
		}
	}

	// Can receive messages
	{
		_, err := conn.Do("PUBLISH", "pattern:banana", "Hello World 1!")
		if err != nil {
			panic(err)
		}

		_, err = conn.Do("PUBLISH", "pattern:peach", "Hello World 2!")
		if err != nil {
			panic(err)
		}

		timer := time.NewTimer(3 * time.Second)
		for i := 1; i <= 2; i += 1 {
			select {
			case m := <-messages:
				mm := fmt.Sprintf("Hello World %v!", i)
				if string(m) != mm {
					t.Errorf("Messages are not equal. Got [%v], expected [%v]", string(m), mm)
				}
			case <-timer.C:
				t.Errorf("timed out while waiting for messages")
			}
		}
	}

	sub.Close()

}
