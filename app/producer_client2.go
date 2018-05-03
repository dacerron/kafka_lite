/*
	Simple client

	USAGE:
	go run producer_client2.go [server-addr]
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

	s := "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has"
	fmt.Println("Writing message: ", s)
	copy(message[:], s)
	err = p.AddMessageTimeout(0, message, 30)
	checkError(err)

	message = [256]byte{}
	s = "survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop"
	fmt.Println("Writing message: ", s)
	copy(message[:], s)
	err = p.AddMessageTimeout(0, message, 30)
	checkError(err)

	message = [256]byte{}
	s = "publishing software like Aldus PageMaker including versions of Lorem Ipsum."
	fmt.Println("Writing message: ", s)
	copy(message[:], s)
	err = p.AddMessageTimeout(0, message, 30)
	checkError(err)

	message = [256]byte{}
	s = "It is a long established fact that a reader will be distracted by the readable content of a page when looking at its layout. The point of using Lorem Ipsum is that it has a more-or-less normal distribution of letters, as opposed to using 'Content here,"
	fmt.Println("Writing message: ", s)
	copy(message[:], s)
	err = p.AddMessageTimeout(0, message, 30)
	checkError(err)

	message = [256]byte{}
	s = "content here', making it look like readable English. Many desktop publishing packages and web page editors now use Lorem Ipsum as their default model text, and a search for 'lorem ipsum' will uncover many web sites still in their infancy. Various"
	fmt.Println("Writing message: ", s)
	copy(message[:], s)
	err = p.AddMessageTimeout(0, message, 30)
	checkError(err)

	message = [256]byte{}
	s = "versions have evolved over the years, sometimes by accident, sometimes on purpose (injected humour and the like)."
	fmt.Println("Writing message: ", s)
	copy(message[:], s)
	err = p.AddMessageTimeout(0, message, 30)
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
