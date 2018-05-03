/*
	Simple consumer client

	USAGE:
	go run consumer_client.go [server-addr]
*/

package main

import (
	"fmt"
	"os"
	"stash.ugrad.cs.ubc.ca/proj2_j9x9a_l9z9a_q7z9a_z1e9/consumer"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run consumer_client.go [server-addr]")
		os.Exit(1)
	}

	serverAddr := os.Args[1]
	// partitionIds := []uint8{1, 2, 3} // may want to change this for testing
	numConsumers := 2

	// fmt.Printf("Starting client to read ids: %v. Connecting to %s...\n\n", partitionIds, serverAddr)
	c, err := consumerlib.MountConsumerGroup(serverAddr, numConsumers)
	checkError(err)
	fmt.Println("Mount succeeded")

	for {
		var records []consumerlib.ConsumerRecord
		records = c.Poll(1000)
		for _, record := range records {
			fmt.Printf("Received: '%s' from partition %d offset %d\n", string(record.Value), record.PartitionId, record.Offset)
		}
	}

	// offset := uint(1)
	// n := uint(1)
	// fmt.Printf("\n\nReading %d messages at offset %d\n", n, offset)
	// msgs, err := c.Consume(partitionIds, offset, n)
	// checkError(err)
	// fmt.Println("Messages: ", msgs)

	err = c.Unmount()
	fmt.Println("\n\nFinished")
}

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
