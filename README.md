# go-mpx
RedisMPX implementation for the Go programming language.

## Abstract
RedisMPX is a Redis Pub/Sub multiplexer written in multiple languages and livecoded on Twitch.

- [Twitch channel](https://twitch.tv/kristoff_it)
- [YouTube VOD archive](https://www.youtube.com/user/Kappaloris/videos)


## Status
This library is currently in Alpha, still main features missing. Don't use in production.



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
	connBuilder := func() redis.Conn {
		conn, err := redis.Dial("tcp", ":6379")
		if err != nil {
			panic(err)
		}
		return conn
	}

	// Create a Multiplexer
	multiplexer := mpx.New(connBuilder)

	// Create a Subscription
	sub := multiplexer.NewSubscription(func(ch string, msg []byte) {
		fmt.Printf("channel: [%v] msg: [%v]\n", ch, string(msg))
	})

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
