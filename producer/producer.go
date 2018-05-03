/*

This library exposes interface to the Producer API.

*/

package producer

import (
	"fmt"
	"github.com/DistributedClocks/GoVector/govec"
	"stash.ugrad.cs.ubc.ca/proj2_j9x9a_l9z9a_q7z9a_z1e9/networking"
	"strings"
	"time"
)

////////////////////////////////////////////////////////////////////////////////////////////
// <ERROR DEFINITIONS>

// Contains serverAddr
type DisconnectedError string

func (e DisconnectedError) Error() string {
	return fmt.Sprintf("Producer: Not connnected to server [%s]", string(e))
}

type LeaderNotFoundError string

func (e LeaderNotFoundError) Error() string {
	return fmt.Sprintf("Producer: Leader not found at partitionId [%s]", string(e))
}

type BrokerUnavailableError string

func (e BrokerUnavailableError) Error() string {
	return fmt.Sprintf("Producer: No brokers available from the server")
}

type BrokerDisconnectError string

func (e BrokerDisconnectError) Error() string {
	return fmt.Sprintf("Producer: Disconnected from Broker IP: ", string(e))
}

type InvalidPartitionIdError string

func (e InvalidPartitionIdError) Error() string {
	return fmt.Sprintf("Producer: Invalid PartitionId: ", string(e))
}

type WriteFailedError string

func (e WriteFailedError) Error() string {
	return fmt.Sprintf("Producer: Failed to write to partition with partitionId: ", string(e))
}

// </ERROR DEFINITIONS>
////////////////////////////////////////////////////////////////////////////////////////////

type Producer interface {
	AddMessage(partitionId int, message [256]byte) (err error)

	MountProducer(serverAddr string) (err error)
}

// implementation of Producer interface
// holds information about the producer
type ProducerInstance struct {
	serverConn *networking.Conn
	brokerConn *networking.Conn // maybe we want to have more than one of these? for now the producer will have just one at a time
	logger     *govec.GoLog
}

type WriteArgs struct {
	PartitionId int
	Data        [256]byte
}

func (p *ProducerInstance) AddMessage(partitionId int, data [256]byte) error {
	err := p.getBroker()
	if err != nil {
		return err
	}

	p.logger.LogLocalEvent("Add Message to a Partition")

	leaderIp, err := p.GetLeaderIp(partitionId)
	if err == InvalidPartitionIdError(partitionId) {
		return InvalidPartitionIdError(partitionId)
	}

	fmt.Println("Producer: successfully retrieved leaderIp: ", leaderIp)
	p.logger.LogLocalEvent("Add Message to a Partition")

	err = p.WriteToPartition(partitionId, data, leaderIp)
	if err == WriteFailedError(partitionId) {
		return WriteFailedError(partitionId)
	} else if err == BrokerDisconnectError(leaderIp) {
		return BrokerDisconnectError(leaderIp)
	}

	fmt.Println("Producer: successfully wrote to partitionId: ", partitionId)

	return nil
}

func (p *ProducerInstance) AddMessageTimeout(partitionId int, data [256]byte, timeout int) error {
	start := time.Now()
	var err error
	for time.Now().Sub(start) < time.Duration(timeout)*time.Second {
		err = p.AddMessage(partitionId, data)
		if err == nil {
			return nil
		} else {
			time.Sleep(1 * time.Second)
		}
	}
	return err
}

func (p *ProducerInstance) GetLeaderIp(partitionId int) (string, error) {
	fmt.Println("Producer: in GetLeaderIp")

	var leaderIpArr []string
	var reply []byte
	args := p.logger.PrepareSend(fmt.Sprintf("Get Leader Ip of partitionId: %d", partitionId), partitionId)

	// err := brokerConn.Conn.Call("Broker.GetLeaderIp", &partitionId, &leaderIp)
	err := p.brokerConn.Conn.Call("Broker.GetLeaderIp", &args, &reply)
	p.logger.UnpackReceive("Received Leader Ip from Broker", reply, &leaderIpArr)

	if err != nil {
		switch err.(type) {
		case *LeaderNotFoundError:
			return "", LeaderNotFoundError(partitionId)
		default:
			return "", BrokerDisconnectError(p.brokerConn.GetAddr())
		}
	}
	return leaderIpArr[0], nil
}

func (p *ProducerInstance) WriteToPartition(partitionId int, data [256]byte, leaderIp string) error {
	fmt.Println("Producer: in WriteToPartition")

	writeArgs := WriteArgs{partitionId, data}
	args := p.logger.PrepareSend("Requesting to write message to partition", writeArgs)
	var _ignored bool

	leaderConn, err := networking.CreateConn(leaderIp) // contact broker that is leader of partition
	if err != nil {
		return BrokerDisconnectError(leaderIp)
	}

	err = leaderConn.Conn.Call("Broker.WriteToPartition", &args, &_ignored)
	if err == WriteFailedError(partitionId) {
		return WriteFailedError(partitionId)
	} else if err == BrokerDisconnectError(leaderIp) {
		return BrokerDisconnectError(leaderIp)
	}

	return nil
}

// Closes connection to server
func (p *ProducerInstance) UnMount() error {
	err := p.brokerConn.Conn.Close()
	return err
}

func MountProducer(serverAddr string) (ProducerInstance, error) {
	serverConn, err := networking.CreateConn(serverAddr)
	if err != nil {
		return ProducerInstance{}, DisconnectedError(serverAddr)
	}

	//   var _ignored bool
	//   var brokerAddr string
	//   err = serverConn.Conn.Call("Server.GetBrokerAddr", &_ignored, &brokerAddr)
	//   if err != nil {
	//     return ProducerInstance{}, DisconnectedError(serverAddr)
	//   }

	//   brokerConn, err := networking.CreateConn(brokerAddr)
	//   if err != nil {
	// 	  return ProducerInstance{}, BrokerUnavailableError("")
	//   }
	//   fmt.Println("connected to a broker:", brokerConn.GetAddr())

	ip := networking.GetOutboundIP()
	id := strings.Replace(ip, ".", "", -1)

	logger := govec.InitGoVector(id, id)
	return ProducerInstance{serverConn: serverConn, logger: logger}, nil
}

func (p *ProducerInstance) getBroker() error {
	_ignored := false
	brokerAddr := ""
	err := p.serverConn.Conn.Call("Server.GetBrokerAddr", &_ignored, &brokerAddr)
	if err != nil {
		return BrokerUnavailableError("")
	}
	brokerConn, err := networking.CreateConn(brokerAddr)
	if err != nil {
		return err
	}
	p.brokerConn = brokerConn
	return nil
}

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
	}
}
