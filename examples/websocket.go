package main

import (
	"fmt"
	"github.com/RedisMPX/go-mpx"
	"github.com/gomodule/redigo/redis"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
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

	connBuilder := func() redis.Conn {
		conn, err := redis.Dial("tcp", ":6379")
		if err != nil {
			panic(err)
		}
		return conn
	}

	multiplexer := mpx.New(connBuilder)

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println(err)
			return
		}

		sub := multiplexer.NewSubscription(func(ch string, msg []byte) {
			if err := websocket.WriteJSON(conn, string(msg)); err != nil {
				log.Println(err)
				return
			}
		})

		// TODO: abstract the current client away so that you can implement some error reporting
		// (or try to get the current client to tell you by using the other, lower-level, API)
		// sub.SetOnReconnect(func() {
		// 	if err := websocket.WriteJSON(conn, "<Reconnected>"); err != nil {
		// 		log.Println(err)
		// 		return
		// 	}
		// })

		// Start the reader gorotuine associated with this WS.
		go func(conn *websocket.Conn) {
			for {
				_, p, err := conn.ReadMessage()
				if err != nil {
					// Clear the subscription when there's an error (assuming the ws
					// connection died)
					sub.Clear()
					log.Println(err)
					return
				}

				if len(p) == 0 {
					continue
				}

				ch := string(p[1:])

				switch p[0] {
				case '+':
					fmt.Printf("[ws] Wants to join [%s]\n", ch)
					sub.Add(ch)
				case '-':
					fmt.Printf("[ws] Wants to leave [%s]\n", ch)
					sub.Remove(ch)
				case '!':
					fmt.Println("[ws] requested to kill the client")
					// TODO: what happens with redigo?
					// rClient.Close()
				case '?':
					fmt.Printf("[ws] active subscriptions: {%v}", sub.GetChannels())
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
