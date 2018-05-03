package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"stash.ugrad.cs.ubc.ca/proj2_j9x9a_l9z9a_q7z9a_z1e9/consumer"
)

func consumeHandler(res http.ResponseWriter, req *http.Request) {
	res.Header().Set("Content-Type", "application/json")
	res.Header().Set("Access-Control-Allow-Origin", "*")
	if err != nil {
		res.WriteHeader(400)
	} else {
		jsonmsg, err := json.Marshal(msgRecord)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(string(jsonmsg))
		// msgRecord = nil
		// msgRecord = nil
		// res.Write(jsonmsg)

		res.Write(jsonmsg)
		msgRecord = nil
	}
}

var err error
var msgRecord []consumerlib.ConsumerRecord

func main() {
	if len(os.Args) != 3 {
            fmt.Println("usage: go run consumer_doggo.go [server-addr] [public-ip:port]")
		os.Exit(1)
	}

	serverAddr := os.Args[1]
    addr := os.Args[2]
	numConsumers := 2

	c, err := consumerlib.MountConsumerGroup(serverAddr, numConsumers)
	fmt.Println(err)
	fmt.Println("Mount succeeded")
	fmt.Println("Done with init")
	http.HandleFunc("/consume", consumeHandler)
	fmt.Println("serving on " + addr)
	go func() {
		for {
			var records []consumerlib.ConsumerRecord
			records = c.Poll(1000)
			for _, record := range records {
				msgRecord = append(msgRecord, record)
				fmt.Printf("Added: %+v\n", record)
			}
		}
	}()
	log.Fatal(http.ListenAndServe(addr, nil))

}
