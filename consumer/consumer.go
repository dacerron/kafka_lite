package consumerlib

import (
	// "../networking"
	// "../kafkaIO"
	"fmt"
	"math"
	"net/rpc"
	"runtime"
	"sync"
	"time"
)

var BATCH_SIZE uint = 10
var wg sync.WaitGroup

// ===== ERROR DEFINITIONS =====

// TODO:
/*
	- Disconnected Error
	- Invalid Client ID Error
*/

// =============================

type ConsumerAPI interface {
	Poll(timeout int) []ConsumerRecord

	// Return n messages starting at offset for given Partition ID
	Consume(partitionIds []uint8, offset uint, n uint) (msgs map[uint8][][]byte, err error)

	Unmount() error
}

type ConsumerGroup struct {
	ServerAddr          string
	ServerConn          *rpc.Client
	NumConsumers        int
	PartitionNextOffset []uint
}

type ReadArgs struct {
	PartitionId uint8
	Offset      uint
	N           uint
}

type ConsumerRecord struct {
	PartitionId uint8
	Offset      uint
	Value       []byte
}

// takes in timeout in seconds
func (c ConsumerGroup) Poll(timeout int) []ConsumerRecord {
	start := time.Now()
	for time.Now().Sub(start) < time.Duration(timeout)*time.Second {
		var records []ConsumerRecord
		msgs, err := c.ConsumeAllPartitions()
		checkError(err)
		for partitionId, messageArr := range msgs {
			for i, bArr := range messageArr {
				baseOffset := c.PartitionNextOffset[partitionId] - uint(len(messageArr))
				record := ConsumerRecord{PartitionId: uint8(partitionId), Offset: baseOffset + uint(i), Value: bArr}
				records = append(records, record)
			}
		}
		if len(records) > 0 {
			return records
		} else {
			time.Sleep(2 * time.Second)
		}
	}
	return nil
}

func (c ConsumerGroup) ConsumeAllPartitions() (msgs [][][]byte, err error) {
	result := make([][][]byte, len(c.PartitionNextOffset))
	numConsumers := c.NumConsumers
	partitionLength := len(c.PartitionNextOffset)

	manifests := [][]uint8{}
	acc := 0
	for i := 0; i < numConsumers; i++ {
		if acc >= partitionLength {
			break
		}
		var temp []uint8
		for j := 0; j < int(math.Ceil(float64(partitionLength)/float64(numConsumers))); j++ {
			if acc >= partitionLength {
				break
			}
			temp = append(temp, uint8(acc))
			acc++
		}
		manifests = append(manifests, temp)
	}

	wg.Add(numConsumers)

	// start consumer goroutines
	for i := 0; i < numConsumers; i++ {
		go c.startConsumer(result, manifests[i])
	}

	wg.Wait()

	return result, nil
}

func (c ConsumerGroup) startConsumer(result [][][]byte, manifest []uint8) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in startConsumer", r)
		}
	}()
	defer wg.Done()
	var brokerAddr string
	err := c.ServerConn.Call("Server.GetBrokerAddr", true, &brokerAddr)
	checkError(err)

	brokerConn, err := rpc.Dial("tcp", brokerAddr)
	checkError(err)

	for _, partitionId := range manifest {
		var reply [][]byte
		err := brokerConn.Call("Broker.ReadFromPartition", &ReadArgs{PartitionId: partitionId, Offset: c.PartitionNextOffset[partitionId], N: BATCH_SIZE}, &reply)
		if err == nil {
			c.PartitionNextOffset[partitionId] += uint(len(reply))
			result[partitionId] = reply
		} else {
			fmt.Println(err)
		}
	}
}

// Return n messages starting at offset for given PartitionID
// n = 0 will return all messages
// TODO: fix this later to return a Message type
func (c ConsumerGroup) Consume(partitionIds []uint8, offset uint, n uint) (msgs map[uint8][][]byte, err error) {

	result := make(map[uint8][][]byte)
	numConsumers := c.NumConsumers
	partitionLength := len(partitionIds)

	var brokerAddr string
	err = c.ServerConn.Call("Server.GetBrokerAddr", true, &brokerAddr)
	checkError(err)
	if err != nil {
		return result, err
	}

	// connect to broker
	brokerConn, err := rpc.Dial("tcp", brokerAddr)
	checkError(err)
	defer brokerConn.Close()

	// generate manifest for each consumer
	manifests := make([][]uint8, numConsumers)
	acc := 0
	for i := 0; i < numConsumers; i++ {
		if acc >= partitionLength {
			break
		}
		var temp []uint8
		for j := 0; j < int(math.Ceil(float64(partitionLength)/float64(numConsumers))); j++ {
			if acc >= partitionLength {
				break
			}
			temp = append(temp, partitionIds[acc])
			acc++
		}
		manifests = append(manifests, temp)
	}

	// set max processes (threads)
	oldMaxProcs := runtime.GOMAXPROCS(numConsumers)

	// start consumer goroutines
	for i := 0; i < numConsumers; i++ {
		go startConsumer(result, manifests[i], offset, n, brokerConn, i)
	}

	// revert maxprocs to old value
	runtime.GOMAXPROCS(oldMaxProcs)

	return result, nil
}

// Closes connection to server
func (c ConsumerGroup) Unmount() error {
	err := c.ServerConn.Close()
	return err
}

func startConsumer(result map[uint8][][]byte, manifest []uint8, offset uint, n uint, brokerConn *rpc.Client, i int) {

	for _, partitionId := range manifest {
		var reply [][]byte
		err := brokerConn.Call("Broker.ReadFromPartition", &ReadArgs{PartitionId: partitionId, Offset: offset, N: n}, &reply)

		if err != nil {
			// check error
			fmt.Printf("[READING PARTITIONS %d ON CONSUMER %d] ", partitionId, i)
			fmt.Println(err)
		}

		result[partitionId] = reply
	}

}

// Returns an instance of the ConsumerGroup API
func MountConsumerGroup(serverAddr string, numConsumers int) (cg ConsumerAPI, err error) {
	runtime.GOMAXPROCS(numConsumers)
	serverConn, err := rpc.Dial("tcp", serverAddr)
	checkError(err)

	var brokerAddr string
	err = serverConn.Call("Server.GetBrokerAddr", true, &brokerAddr)
	checkError(err)

	brokerConn, err := rpc.Dial("tcp", brokerAddr)
	checkError(err)

	var _ignored bool
	var numOfPartitions uint8
	err = brokerConn.Call("Broker.GetNumOfPartitions", &_ignored, &numOfPartitions)

	partitions := make([]uint, numOfPartitions)

	cg = &ConsumerGroup{ServerAddr: serverAddr, ServerConn: serverConn, NumConsumers: numConsumers, PartitionNextOffset: partitions}
	return cg, nil
}

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
	}
}
