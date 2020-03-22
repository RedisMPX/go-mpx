# go-mpx
RedisMPX implementation for the Go programming language.

## Abstract
RedisMPX is a Redis Pub/Sub multiplexer written in multiple languages and livecoded on Twitch.

- [Twitch channel](https://twitch.tv/kristoff_it)
- [YouTube VOD archive](https://www.youtube.com/user/Kappaloris/videos)


## Status
Main functionality completed, needs testing.

## Features
- Simple channel subscriptions
- Pattern subscriptions
- Networked promise system
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
	sub := multiplexer.NewSubscription(onMessage, nil, nil)

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
