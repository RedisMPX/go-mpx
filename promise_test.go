package mpx

import (
	"github.com/gomodule/redigo/redis"
	"testing"
	"time"
)

func TestPromise(t *testing.T) {
	connBuilder := func() (redis.Conn, error) {
		return redis.Dial("tcp", ":6379")
	}

	conn, err := connBuilder()
	if err != nil {
		panic(err)
	}

	multiplexer := New(connBuilder)

	sub := multiplexer.NewPromiseSubscription("promise-")

	timer := time.AfterFunc(3*time.Second, func() {
		sub.Close()
	})

	// Can activate
	if ok := sub.WaitForActivation(); !ok {
		t.Errorf("failed to activate in time")
	}

	// Can create promises
	{

		promiseBanana, err := sub.NewPromise("banana", 3*time.Second)
		if err != nil {
			panic(err)
		}
		_, err = conn.Do("PUBLISH", "promise-banana", "Hello World banana!")
		if err != nil {
			panic(err)
		}
		select {
		case m := <-promiseBanana.C:
			mm := "Hello World banana!"
			if string(m) != mm {
				t.Errorf("Messages are not equal. Got [%v], expected [%v]", string(m), mm)
			}
		case <-timer.C:
			t.Errorf("timed out while waiting for messages")
		}
	}

}
