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

		onMessage := func(ch string, msg []byte) {
			if err := websocket.WriteJSON(conn, string(msg)); err != nil {
				log.Println(err)
				return
			}
		}

		onDisconnect := func(_ error) {
			if err := websocket.WriteJSON(conn, "*Disconnected*"); err != nil {
				log.Println(err)
				return
			}
		}

		onActivation := func(name string) {
			if err := websocket.WriteJSON(conn, fmt.Sprintf("*Activated: [%v]*", name)); err != nil {
				log.Println(err)
				return
			}
		}

		sub := multiplexer.NewChannelSubscription(onMessage, onDisconnect, onActivation)

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
				case '+':
					fmt.Printf("[ws] Wants to join [%s]\n", ch)
					sub.Add(ch)
				case '-':
					fmt.Printf("[ws] Wants to leave [%s]\n", ch)
					sub.Remove(ch)
				case '!':

				case '?':
					fmt.Printf("[ws] active subscriptions: {%v}", sub.GetChannels())
				case 'q':
					multiplexer.Stop()
					println("yep the multiplexer was closed")
				case 'r':
					multiplexer.Restart()
					println("running!")
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
