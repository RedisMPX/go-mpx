# go-mpx
RedisMPX is a Redis Pub/Sub multiplexer library written in multiple languages and [live coded on Twitch](https://twitch.tv/kristoff_it).

The live coding of this implementation is [archived on YouTube](https://www.youtube.com/watch?v=w6CyJ5dlFd4&list=PLVnMKELF_8IRJ37dW799IxkztIm1Dk3Az).

## Abstract
When bridging multiple application instances through Redis Pub/Sub it's easy to end up needing
support for multiplexing. RedisMPX streamlines this process in a consistent way across multiple
languages by offering a consistent set of features that cover the most common use cases.

The library works under the assumption that you are going to create separate subscriptions
for each client connected to your service (e.g. WebSockets clients):

- ChannelSubscription allows you to add and remove individual Redis
  PubSub channels similarly to how a multi-room chat application would need to.
- PatternSubscription allows you to subscribe to a single Redis Pub/Sub pattern.
- PromiseSubscription allows you to create a networked promise system.

## Features
- Simple channel subscriptions
- Pattern subscriptions
- **[Networked promise system](https://godoc.org/github.com/RedisMPX/go-mpx#Promise)**
- Connection retry with exponetial backoff + jitter
- Aggressive pipelining over the Redis Pub/Sub connection

## Documentation
- [API Reference](https://godoc.org/github.com/RedisMPX/go-mpx)
- [Examples](/examples/)

## Quickstart
```go
package main

import (
	"fmt"
	"github.com/RedisMPX/go-mpx"
	"github.com/gomodule/redigo/redis"
	"time"
)

func main() {
	connBuilder := func() (redis.Conn, error) {
		return redis.Dial("tcp", ":6379")
	}

	// Create a Multiplexer
	multiplexer := mpx.New(connBuilder)

	// Define a onMessage callback
	onMessage := func(ch string, msg []byte) {
		fmt.Printf("channel: [%v] msg: [%v]\n", ch, string(msg))
	}

	// Create a Subscription
	sub := multiplexer.NewChannelSubscription(onMessage, nil, nil)

	// Add a Redis Pub/Sub channel to the Subscription
	sub.Add("mychannel")

	// Create a second connection to Redis
	conn, err := redis.Dial("tcp", ":6379")
	if err != nil {
		panic(err)
	}

	// Publish Messages over Redis Pub/Sub forever.
	for {
		_, err := conn.Do("PUBLISH", "mychannel", "Hello World!")
		if err != nil {
			panic(err)
		}

		time.Sleep(3 * time.Second)
	}
}
```
