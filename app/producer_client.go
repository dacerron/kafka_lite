/*
	Simple client

	USAGE:
	go run producer_client.go [server-addr]
*/

package main

import (
	"fmt"
	"os"
	"stash.ugrad.cs.ubc.ca/proj2_j9x9a_l9z9a_q7z9a_z1e9/producer"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run producer_client.go [server-addr]")
		os.Exit(1)
	}

	serverAddr := os.Args[1]
	p, err := producer.MountProducer(serverAddr)
	checkError(err)
	fmt.Println("Done with init")

	var message [256]byte

	s := "I want to be the 1st doggo!!"
	fmt.Println("Writing message: ", s)
	copy(message[:], s)
	err = p.AddMessageTimeout(0, message, 30)
	checkError(err)

	message = [256]byte{}
	s = "I want to be the 2nd doggo!!"
	fmt.Println("Writing message: ", s)
	copy(message[:], s)
	err = p.AddMessageTimeout(1, message, 30)
	checkError(err)

	message = [256]byte{}
	s = "I, doggo2 like to dig!!"
	fmt.Println("Writing message: ", s)
	copy(message[:], s)
	err = p.AddMessageTimeout(1, message, 30)
	checkError(err)

	message = [256]byte{}
	s = "I want to be the 3rd doggo!!"
	fmt.Println("Writing message: ", s)
	copy(message[:], s)
	err = p.AddMessageTimeout(2, message, 30)
	checkError(err)

	message = [256]byte{}
	s = "I, doggo3 like to play fetch!!"
	fmt.Println("Writing message: ", s)
	copy(message[:], s)
	err = p.AddMessageTimeout(2, message, 30)
	checkError(err)

	message = [256]byte{}
	s = "I, doggo3 also like to sleep!!"
	fmt.Println("Writing message: ", s)
	copy(message[:], s)
	err = p.AddMessageTimeout(2, message, 30)
	checkError(err)

	fmt.Println("UnMounting producer broker connection...")
	err = p.UnMount()
	checkError(err)
	fmt.Println("\n\nFinished")
}

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
	}
}
