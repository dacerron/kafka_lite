package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"stash.ugrad.cs.ubc.ca/proj2_j9x9a_l9z9a_q7z9a_z1e9/producer"
	"time"
    "strconv"
)

func produceHandler(res http.ResponseWriter, req *http.Request) {
	message := req.URL.Query()["message"]
    pid, err := strconv.Atoi(req.URL.Query()["pid"][0])
    if err != nil {
        fmt.Println(err)
    }
	bytearr := [256]byte{}
	copy(bytearr[:], message[0])
	err = p.AddMessageTimeout(pid, bytearr, 30)
	if err != nil {
		fmt.Println(err)
		res.WriteHeader(400)
	} else {
		res.WriteHeader(200)
	}
}

var p producer.ProducerInstance
var err error

func main() {
	if len(os.Args) != 3 {
		fmt.Println("usage: go run producer_client.go [server-addr] [public-addr]")
		os.Exit(1)
	}

	serverAddr := os.Args[1]
    addr := os.Args[2]
	p, err = producer.MountProducer(serverAddr)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Done with init")
	http.HandleFunc("/produce", produceHandler)
	fmt.Println("serving on " + addr)
	log.Fatal(http.ListenAndServe(addr, nil))
	time.Sleep(time.Hour)
}
