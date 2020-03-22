package main

import (
	"fmt"
	"github.com/RedisMPX/go-mpx"
	"github.com/gomodule/redigo/redis"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"time"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func main() {

	// rClient := redis.NewClient(&redis.Options{
	// 	Addr:     "localhost:6379", // use default Addr
	// 	Password: "",               // no password set
	// 	DB:       0,                // use default DB
	// })

	connBuilder := func() (redis.Conn, error) {
		return redis.Dial("tcp", ":6379")
	}

	multiplexer := mpx.New(connBuilder)

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println(err)
			return
		}

		sub := multiplexer.NewPromiseSubscription("p:")
		sub.WaitForActivation()
		println("sub is active!")

		// Start the reader gorotuine associated with this WS.
		go func(conn *websocket.Conn) {
			defer sub.Close()
			for {
				_, p, err := conn.ReadMessage()
				if err != nil {
					log.Println(err)
					return
				}

				if len(p) == 0 {
					continue
				}

				ch := string(p[1:])

				switch p[0] {
				case '*':
					promise, _ := sub.WaitForNewPromise(ch, 5*time.Second)
					println("OK")
					go func(p *mpx.Promise, ch string) {
						if ch == "killme" {
							p.Cancel()
						}
						for {
							msg, ok := <-p.C
							if ok {
								fmt.Printf("[promise %v] Received [%v]\n", ch, string(msg))
							} else {
								fmt.Printf("[promise %v] Channel closed\n", ch)
								break
							}
						}
					}(promise, ch)
				case '+':
					promise, err := sub.NewPromise(ch, 5*time.Second)
					println("OK")
					if err != nil {
						println("failed to create the promise: we are disconnected")
						continue
					}
					go func(p *mpx.Promise, ch string) {
						if ch == "killme" {
							p.Cancel()
						}
						for {
							msg, ok := <-p.C
							if ok {
								fmt.Printf("[promise %v] Received [%v]\n", ch, string(msg))
							} else {
								fmt.Printf("[promise %v] Channel closed\n", ch)
								break
							}
						}
					}(promise, ch)
				default:
					continue
				}

			}
		}(conn)

	})

	err := http.ListenAndServe("127.0.0.1:7778", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
