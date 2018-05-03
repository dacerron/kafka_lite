package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/DistributedClocks/GoVector/govec"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"stash.ugrad.cs.ubc.ca/proj2_j9x9a_l9z9a_q7z9a_z1e9/consumer"
	"stash.ugrad.cs.ubc.ca/proj2_j9x9a_l9z9a_q7z9a_z1e9/kafkaIO"
	"stash.ugrad.cs.ubc.ca/proj2_j9x9a_l9z9a_q7z9a_z1e9/networking"
	"stash.ugrad.cs.ubc.ca/proj2_j9x9a_l9z9a_q7z9a_z1e9/producer"
	"strconv"
	"sync"
	"time"
)

type Config struct {
	NumOfPartitions uint8 `json:"num-of-partitions"`
}

// holds addresses all the other brokers with broker knows about
type Peers struct {
	sync.RWMutex
	all map[string]*networking.Conn
}

// holds metadata about each partition
type Partition struct {
	sync.RWMutex
	isLeader         bool
	brokerLeaderIP   string
	electionNominees []string
}

// holds information about this broker
type Broker struct {
	ID         int
	brokerAddr string
	peers      Peers
	partitions []*Partition
	logger     *govec.GoLog
}

func (B *Broker) HeartBeat(peer *string, _ignored *bool) error {
	B.peers.Lock()
	defer B.peers.Unlock()
	conn, ok := B.peers.all[*peer]
	if ok {
		networking.UpdateRecentHeartBeat(conn)
	}
	return nil
}

func (B *Broker) AddPeer(newAddr *string, reply *int) error {
	B.peers.Lock()
	defer B.peers.Unlock()
	newConn, err := networking.CreateConn(*newAddr)
	if err != nil {
		return err
	}
	B.peers.all[*newAddr] = newConn
	*reply = 1
	fmt.Println("Peer added successfully:", *newAddr)
	return nil
}

func (B *Broker) GetNumOfPartitions(_ignored *bool, reply *uint8) error {
	*reply = config.NumOfPartitions
	return nil
}

func (B *Broker) GetPartitionState(args *[]byte, reply *[]byte) error {
	var _ignored bool
	B.logger.UnpackReceive("Received Message to share partition state", *args, &_ignored)
	leaderIPs := make([]string, len(B.partitions))
	for i, p := range B.partitions {
		p.Lock()
		leaderIPs[i] = p.brokerLeaderIP
		p.Unlock()
	}
	*reply = B.logger.PrepareSend("Sending my partition state to my peer", leaderIPs)
	return nil
}

func (B *Broker) ShareLeaderLoad(args *[]byte, _ignored *bool) error {
	var newLeaderIP string
	B.logger.UnpackReceive("Received Message to share leader load", *args, &newLeaderIP)
	// TODO: need to add validation before giving up leadership
	var partitionId uint8
	for i, partition := range B.partitions {
		if partition.isLeader {
			partitionId = uint8(i)
			partition.Lock()
			partition.isLeader = false
			partition.brokerLeaderIP = newLeaderIP
			defer partition.Unlock()
			break
		}
	}

	fmt.Printf("[ShareLeaderLoad] Will be giving up partition %d to broker ip %s\n", partitionId, newLeaderIP)

	B.peers.Lock()
	for _, peer := range B.peers.all {
		leaderDataArgs := LeaderDataArgs{PartitionId: partitionId, LeaderIP: newLeaderIP}
		args := B.logger.PrepareSend("Broadcasting Update Leader", leaderDataArgs)
		err := peer.Conn.Call("Broker.UpdateLeader", &args, &_ignored)
		checkError(err)
	}
	B.peers.Unlock()

	// B.printPartitionState()

	return nil
}

type LeaderDataArgs struct {
	PartitionId uint8
	LeaderIP    string
}

func (B *Broker) UpdateLeader(bArr []byte, _ignored *bool) error {
	var args LeaderDataArgs
	B.logger.UnpackReceive("Received Update Leader Request", bArr, &args)

	partition := B.partitions[args.PartitionId]
	if partition.brokerLeaderIP != args.LeaderIP {
		partition.Lock()
		partition.brokerLeaderIP = args.LeaderIP
		if args.LeaderIP == B.brokerAddr {
			partition.isLeader = true
		}
		partition.Unlock()

		// Broadcast to peers that leader is updated, ensuring consistency in leadership state
		for _, peer := range B.peers.all {
			leaderDataArgs := B.logger.PrepareSend("Broadcating Update Leader", args)
			err := peer.Conn.Call("Broker.UpdateLeader", leaderDataArgs, _ignored)
			checkError(err)
		}
		// B.printPartitionState()
	}
	return nil
}

func (B *Broker) broadcastElectionDecision(ip string, partitionID uint8) {
	for _, peer := range B.peers.all {
		var _ignored bool
		args := LeaderDataArgs{PartitionId: partitionID, LeaderIP: ip}
		electionNomineeArgs := B.logger.PrepareSend("Broadcasting Election Decision", args)
		err := peer.Conn.Call("Broker.UpdateElectionNominee", electionNomineeArgs, &_ignored)
		checkError(err)
	}
}

func (B *Broker) UpdateElectionNominee(bArr []byte, _ignored *bool) error {
	var args LeaderDataArgs
	B.logger.UnpackReceive("Received Update Election Decision to update list of nominees", bArr, &args)

	partition := B.partitions[args.PartitionId]
	partition.Lock()
	partition.electionNominees = append(partition.electionNominees, args.LeaderIP)
	partition.Unlock()

	return nil
}

type ElectionData struct {
	BrokerID        int
	IpAddress       string
	PartitionLength int
	LeaderCount     int
}

func (B *Broker) startLeaderElection(partitionID uint8) error {
	fmt.Printf("Start Leader Election for partition %d\n", partitionID)
	B.logger.LogLocalEvent(fmt.Sprintf("Start Leader Election for partition %d", partitionID))
	var votedLeaderIp string
	partition := B.partitions[partitionID]
	partition.Lock()
	partition.brokerLeaderIP = ""
	partition.Unlock()

	for {
		var electionDataArr []ElectionData
		// Start gathering election data from all peers
		for {
			electionDataArr = []ElectionData{}
			B.peers.Lock()
			for _, peer := range B.peers.all {
				var data ElectionData
				args := B.logger.PrepareSend("Retrieve election data from peer", partitionID)
				var reply []byte
				peer.Conn.Call("Broker.RetrieveElectionData", &args, &reply)
				B.logger.UnpackReceive("Received election data from peer", reply, &data)

				if (data != ElectionData{}) {
					electionDataArr = append(electionDataArr, data)
				}
			}
			B.peers.Unlock()

			if len(electionDataArr) != len(B.peers.all) {
				fmt.Println("election data array length ", len(electionDataArr))
				fmt.Println("number of peers ", len(B.peers.all))
				time.Sleep(time.Second * 1)
			} else {
				break
			}
		}

		myElectionData, err := B.createElectionData(partitionID)
		checkError(err)
		electionDataArr = append(electionDataArr, myElectionData)
		fmt.Println("Gathering election data is done: ", electionDataArr)

		leaderIP := decideElectionResult(electionDataArr)
		B.broadcastElectionDecision(leaderIP, partitionID)

		// Collect election nominees from every peers
		for {
			if len(partition.electionNominees) != len(B.peers.all) {
				fmt.Println("Waiting for election result from peers")
				fmt.Println("election nominees length ", len(partition.electionNominees))
				fmt.Println("number of peers ", len(B.peers.all))
				time.Sleep(time.Second * 1)
			} else {
				break
			}
		}

		partition.Lock()
		partition.electionNominees = append(partition.electionNominees, leaderIP)
		partition.Unlock()

		//Check that there is a majority consensus of who is the leader
		freqMap := make(map[string]int)
		for _, ip := range partition.electionNominees {
			_, exist := freqMap[ip]
			if exist {
				freqMap[ip] += 1
			} else {
				freqMap[ip] = 1
			}
		}

		var majority = len(B.peers.all) / 2
		for k, v := range freqMap {
			if v > majority {
				// found majority!
				votedLeaderIp = k
			}
		}

		// Delete election nominees for next election
		partition.Lock()
		partition.electionNominees = []string{}
		partition.Unlock()

		if votedLeaderIp != "" {
			fmt.Println(votedLeaderIp + " is the new leader!")
			B.logger.LogLocalEvent(fmt.Sprintf("Peer with ip %s is the leader of partition %d", votedLeaderIp, partitionID))
			break
		}
		// If no consensus, restart leader election
	}

	// If I am the leader, broadcast it to everyone
	if votedLeaderIp == B.brokerAddr {
		partition.Lock()
		partition.brokerLeaderIP = B.brokerAddr
		partition.isLeader = true
		partition.Unlock()

		for _, peer := range B.peers.all {
			leaderDataArgs := LeaderDataArgs{PartitionId: partitionID, LeaderIP: votedLeaderIp}
			args := B.logger.PrepareSend("Broadcasting Update Leader", leaderDataArgs)
			var _ignored bool
			err := peer.Conn.Call("Broker.UpdateLeader", &args, &_ignored)
			checkError(err)
		}
	}

	B.printPartitionState()

	return nil
}

// Also triggers leader election if it's not going on yet
func (B *Broker) RetrieveElectionData(args *[]byte, reply *[]byte) error {
	var partitionID uint8
	B.logger.UnpackReceive("Received Message to share election data", *args, &partitionID)

	partition := B.partitions[partitionID]
	if partition.brokerLeaderIP != "" {
		partition.Lock()
		partition.brokerLeaderIP = ""
		partition.Unlock()
		go B.startLeaderElection(partitionID)
	}

	electionData, err := B.createElectionData(partitionID)
	if err != nil {
		return err
	}
	*reply = B.logger.PrepareSend("Sending election data to my peer", electionData)
	return nil
}

func (B *Broker) createElectionData(partitionID uint8) (ElectionData, error) {
	filePath := B.getFileName(partitionID)
	partitionLength, err := lineCounter(filePath)
	if err != nil {
		return ElectionData{}, err
	}

	leaderCount := 0
	for _, partition := range B.partitions {
		if partition.isLeader {
			leaderCount++
		}
	}
	electionData := ElectionData{BrokerID: B.ID, IpAddress: B.brokerAddr, PartitionLength: partitionLength, LeaderCount: leaderCount}
	return electionData, nil
}

func decideElectionResult(arr []ElectionData) string {
	var nomineeElectionData []ElectionData
	longestPartitionLength := 0

	// find longestPartitionLength
	for _, data := range arr {
		if data.PartitionLength > longestPartitionLength {
			longestPartitionLength = data.PartitionLength
		}
	}
	for _, data := range arr {
		if data.PartitionLength == longestPartitionLength {
			nomineeElectionData = append(nomineeElectionData, data)
		}
	}

	if len(nomineeElectionData) == 1 {
		// if there is no tie, just return the address
		return nomineeElectionData[0].IpAddress
	} else {
		fmt.Println("Ties! More than one peer have the most up to date partition")
		// tie breaker! Find a broker with minimum leader count
		nomineePool := nomineeElectionData
		nomineeElectionData = []ElectionData{}
		minLeaderCount := int(^uint(0) >> 1) // maxint
		for _, data := range nomineePool {
			if data.LeaderCount < minLeaderCount {
				minLeaderCount = data.LeaderCount
			}
		}
		for _, data := range nomineePool {
			if data.LeaderCount == minLeaderCount {
				nomineeElectionData = append(nomineeElectionData, data)
			}
		}

		if len(nomineeElectionData) == 1 {
			return nomineeElectionData[0].IpAddress
		} else {
			// Another tie breaker! Last resort is to just return the ip address with min IP
			fmt.Println("Ties! More than one peer have the same leader count")
			minBrokerID := int(^uint(0) >> 1)
			index := -1
			for i, data := range nomineeElectionData {
				if data.BrokerID < minBrokerID {
					minBrokerID = data.BrokerID
					index = i
				}
			}
			fmt.Println("Index chosen", index)
			return nomineeElectionData[index].IpAddress
		}
	}
	return ""
}

func (B *Broker) peersHeartBeat() {
	for {
		for _, peer := range B.peers.all {
			err := networking.BrokerHeartBeat(peer, &B.brokerAddr)
			checkError(err)
		}
		time.Sleep(time.Second * 1)
	}
}

func (B *Broker) monitorPeers() {
	for {
		B.peers.Lock()
		for addr, peer := range B.peers.all {
			if !networking.CheckHeartBeat(peer) {
				fmt.Printf("%s timed out\n", addr)
				leaderPartitionIDs := B.leaderOfPartition(addr)
				// a peer can be a leader of multiple partitions, so need to start leader election separately
				for _, id := range leaderPartitionIDs {
					go B.startLeaderElection(uint8(id))
				}
				delete(B.peers.all, addr)
			}
		}
		B.peers.Unlock()
		time.Sleep(time.Second * 2)
	}
}

// return partition IDs in which this peer is leader of
// -1 if it's not leader of any partition
func (B *Broker) leaderOfPartition(brokerIP string) []int {
	var partitionIDs []int
	for i, partition := range B.partitions {
		if partition.brokerLeaderIP == brokerIP {
			partitionIDs = append(partitionIDs, i)
		}
	}
	return partitionIDs
}

func (B *Broker) initKafkaFileSystem() error {
	folderPath := filepath.Join(kafkaIODirPath, strconv.Itoa(B.ID))
	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		os.Mkdir(folderPath, 0700)
	}

	fileArr := make([]*os.File, config.NumOfPartitions)
	var i uint8
	for i = 0; i < config.NumOfPartitions; i++ {
		filePath := B.getFileName(i)
		file, err := os.OpenFile(filePath, os.O_RDONLY|os.O_CREATE, 0666)
		if err != nil {
			return err
		}
		fileArr[i] = file
		defer fileArr[i].Close()
	}
	return nil
}

func (B *Broker) getFileName(partitionID uint8) string {
	return filepath.Join(kafkaIODirPath, strconv.Itoa(B.ID), strconv.Itoa(int(partitionID))+".kafka")
}

func (B *Broker) initPartitionState() {
	B.logger.LogLocalEvent("Init PartitionState")
	B.peers.Lock()
	defer B.peers.Unlock()

	var randomPeer *networking.Conn
	if len(B.peers.all) > 0 {
		i := rand.Intn(len(B.peers.all))
		for _, p := range B.peers.all {
			if i == 0 {
				randomPeer = p
			}
			i--
		}
		var _ignored bool
		var leaderIPs []string
		args := B.logger.PrepareSend("Get Partition state from peer", _ignored)
		var reply []byte
		err := randomPeer.Conn.Call("Broker.GetPartitionState", &args, &reply)
		B.logger.UnpackReceive("Received Partition state from peer", reply, &leaderIPs)

		checkError(err)
		for i, partition := range B.partitions {
			partition.Lock()
			partition.brokerLeaderIP = leaderIPs[i]
			partition.Unlock()
		}
		err = B.replicateAllPartitions()
		checkError(err)
		B.loadBalancePartitionLeader()
	} else {
		for _, partition := range B.partitions {
			partition.Lock()
			partition.isLeader = true
			partition.brokerLeaderIP = B.brokerAddr
			partition.Unlock()
		}
	}
}

func (B *Broker) GetLeaderIp(args *[]byte, reply *[]byte) error {
	var partitionId int
	replyArr := make([]string, 1)

	B.logger.UnpackReceive("Received Message to GetLeaderIp", *args, &partitionId)
	if partitionId < 0 || uint8(partitionId) > config.NumOfPartitions {
		return producer.InvalidPartitionIdError(partitionId)
	}

	if B.partitions[partitionId].brokerLeaderIP != "" {
		replyArr[0] = B.partitions[partitionId].brokerLeaderIP
		*reply = B.logger.PrepareSend("Sending the LeaderIP of the partitionId", replyArr)
		return nil
	} else {
		replyArr[0] = ""
		*reply = B.logger.PrepareSend("LeaderIp not found", replyArr)
		return producer.LeaderNotFoundError(partitionId)
	}
}

func (B *Broker) WriteToPartition(bArr []byte, _ignored *bool) error {
	var args producer.WriteArgs
	B.logger.UnpackReceive("Received Message to WriteToPartition", bArr, &args)
	fmt.Println("Broker: Writing message to partitionId: ", args.PartitionId)

	B.partitions[args.PartitionId].Lock()
	B.peers.Lock()

	if len(B.peers.all) == 0 {
		B.peers.Unlock()

		// immediately write to disk locally
		filePath := B.getFileName(uint8(args.PartitionId))
		kafkaIO.AppendToFile(filePath, args.Data[:])
		B.partitions[args.PartitionId].Unlock()

		return nil
	}

	freqMap := make(map[string]int)
	var counter = 0 // for keeping track of all responses
	var votedLeaderIp = ""

	// 1) Ready to commit to other brokers
	for {
		for _, peer := range B.peers.all {
			var reply []byte
			var leaderIp string
			fmt.Println("Broker: PHASE1 ready to commit")

			readyArgs := B.logger.PrepareSend("Broadcasting PartitionId to get LeaderIP vote", args)
			err := peer.Conn.Call("Broker.ReadyToCommit", readyArgs, &reply)
			checkError(err)

			B.logger.UnpackReceive("Received Voted LeaderIp from peer", reply, &leaderIp)
			// 2) ACKS (leaderIp) FROM THE PEERS:
			// add ack to map, so we can check for frequency of majority vote (aka freq of ip)
			_, exist := freqMap[leaderIp]
			if leaderIp != "" {
				if exist {
					freqMap[leaderIp] += 1
					counter += 1
				} else {
					freqMap[leaderIp] = 1
					counter += 1
				}
			}
		}

		// check if size of slice == peers size
		if counter == len(B.peers.all) {
			break
		} else {
			B.peers.Unlock()
			time.Sleep(time.Second * 1)
			B.peers.Lock()
		}
	}

	var majority = len(B.peers.all) / 2
	for k, v := range freqMap {
		if v > majority {
			// found majority!
			votedLeaderIp = k
		}
	}

	if votedLeaderIp == B.brokerAddr {
		// 3) broadcast to peers so they can replicate the write
		counter = 0
		var consensus bool = true
		for _, peer := range B.peers.all {
			var reply []byte
			var ack bool
			fmt.Println("Broker: PHASE2 request to commit")

			replicateArgs := B.logger.PrepareSend("Broadcasting Request to ReplicateWrite", args)
			err := peer.Conn.Call("Broker.ReplicateWrite", replicateArgs, &reply)
			checkError(err)

			B.logger.UnpackReceive("Received Write ACK from peer", reply, &ack)

			// 4) ACKS (boolean) FROM THE PEERS: append acks to consensus
			consensus = consensus && ack
			counter += 1
		}

		// 4) Keep checking if counter == peers size and consensus
		for {
			if counter == len(B.peers.all) {
				break
			} else {
				time.Sleep(time.Second * 1)
			}
		}

		//TODO: what if consensus is false?

		B.peers.Unlock()

		// 5) write to disk locally
		filePath := B.getFileName(uint8(args.PartitionId))
		kafkaIO.AppendToFile(filePath, args.Data[:])
		B.partitions[args.PartitionId].Unlock()

		B.logger.LogLocalEvent("Successfully wrote Message to Partition")

		return nil
	} else {
		B.peers.Unlock()
		B.partitions[args.PartitionId].Unlock()

		return producer.WriteFailedError(args.PartitionId)
	}
}

func (B *Broker) ReadyToCommit(bArr []byte, reply *[]byte) error {
	var args producer.WriteArgs
	var leaderIp string

	B.logger.UnpackReceive("Received ReadyToCommit Request", bArr, &args)

	fmt.Println("Follower: in ReadyToCommit")
	for index, partition := range B.partitions {
		if index == args.PartitionId {
			leaderIp = partition.brokerLeaderIP
			*reply = B.logger.PrepareSend("Sending Voted LeaderIp to my peer", leaderIp)
		}
	}
	return nil
}

func (B *Broker) ReplicateWrite(bArr []byte, reply *[]byte) error {
	var args producer.WriteArgs
	var ack bool

	B.logger.UnpackReceive("Received ReplicateWrite Request", bArr, &args)

	B.partitions[args.PartitionId].Lock()
	filePath := B.getFileName(uint8(args.PartitionId))
	err := kafkaIO.AppendToFile(filePath, args.Data[:])
	if err != nil {
		ack = false
		*reply = B.logger.PrepareSend("Sending ReplicateWrite ACK to my peer", ack)
		B.partitions[args.PartitionId].Unlock()
		return err
	} else {
		ack = true
		*reply = B.logger.PrepareSend("Sending ReplicateWrite ACK to my peer", ack)
		B.partitions[args.PartitionId].Unlock()
		return nil
	}
}

func (B *Broker) replicateAllPartitions() error {
	for partitionID, partition := range B.partitions {
		if !partition.isLeader {
			partition.Lock()
			defer partition.Unlock()

			filePath := B.getFileName(uint8(partitionID))
			offset, err := lineCounter(filePath)
			if err != nil {
				return err
			}

			bulkSize := 1
			var reply [][]byte

			for {
				args := consumerlib.ReadArgs{uint8(partitionID), uint(offset), uint(bulkSize)}
				B.peers.all[partition.brokerLeaderIP].Conn.Call("Broker.ReadFromPartition", &args, &reply)
				kafkaIO.BulkAppendToFile(filePath, reply)
				if len(reply) == 0 {
					break
				} else {
					offset += bulkSize
					reply = [][]byte{}
				}
			}
		}
	}
	return nil
}

func lineCounter(filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return -1, err
	}
	fileScanner := bufio.NewScanner(file)
	lineCount := 0
	for fileScanner.Scan() {
		lineCount++
	}
	return lineCount, nil
}

// See if this broker can take a leader role of some partition
func (B *Broker) loadBalancePartitionLeader() {
	leaderCountPerBroker := make(map[string]int)
	for _, partition := range B.partitions {
		leaderCountPerBroker[partition.brokerLeaderIP] += 1
	}
	for ip, count := range leaderCountPerBroker {
		if count > (len(B.partitions) / 2) {
			fmt.Println("Requestion to share leader load from ", ip)
			var _ignored bool
			args := B.logger.PrepareSend("Requesting to share leader load", B.brokerAddr)
			err := B.peers.all[ip].Conn.Call("Broker.ShareLeaderLoad", &args, &_ignored)
			checkError(err)
			break
		}
	}
}

func (B *Broker) printPartitionState() {
	fmt.Println("Current Partition State: ")
	for _, p := range B.partitions {
		fmt.Println(p)
	}
}

func (B *Broker) ReadFromPartition(args *consumerlib.ReadArgs, reply *[][]byte) error {
	path := B.getFileName(args.PartitionId)

	resultArray, err := kafkaIO.ReadAtOffset(path, args.Offset, args.N)

	if err != nil {
		switch err.(type) {
		case kafkaIO.OffsetOutOfRangeError:
		default:
			fmt.Println(err)
		}
	}
	// set resultArray
	*reply = resultArray

	return nil
}

var (
	config         Config
	errLog         *log.Logger = log.New(os.Stderr, "[serv] ", log.Lshortfile|log.LUTC|log.Lmicroseconds)
	kafkaIODirPath string
)

func readConfigOrDie(path string) {
	file, err := os.Open(path)
	handleErrorFatal("config file", err)

	buffer, err := ioutil.ReadAll(file)
	handleErrorFatal("read config", err)

	err = json.Unmarshal(buffer, &config)
	handleErrorFatal("parse config", err)
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Not enough CLI argument, run 'go run broker.go [serverAddr] [path to config file] [brokerID]'")
		os.Exit(1)
	}

	serverAddr := os.Args[1]
	configPath := os.Args[2]
	brokerID, err := strconv.Atoi(os.Args[3])
	checkError(err)

	// Reading Config file
	readConfigOrDie(configPath)

	// prepare information about self and start listening
	listener, err := net.Listen("tcp", networking.GetOutboundIP()+":0")
	checkError(err)

	broker := createBroker(listener, brokerID)
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("No caller information")
	}
	kafkaIODirPath = path.Dir(filename)

	err = broker.initKafkaFileSystem()
	checkError(err)

	go rpc.Accept(listener)
	fmt.Println("Broker is listening on :", listener.Addr().String())

	rpc.Register(&broker)
	fmt.Println("registered broker RPC object")

	// Tell server about self
	server, err := rpc.Dial("tcp", serverAddr)
	checkError(err)
	registerReply := 0
	err = server.Call("Server.Register", &broker.brokerAddr, &registerReply)
	checkError(err)
	if registerReply == 1 {
		fmt.Println("successfully registered with server")
	} else {
		fmt.Println("ERROR: Could not register with server")
	}

	go broker.peersHeartBeat()
	go serverHeartBeat(server, &broker.brokerAddr)
	go broker.monitorPeers()

	broker.initPartitionState()
	broker.printPartitionState()

	time.Sleep(time.Hour * 1) // sleep for an hour to let goroutines do their thing
}

func serverHeartBeat(server *rpc.Client, addr *string) {
	for {
		err := networking.ServerHeartBeat(server, addr)
		checkError(err)
		time.Sleep(time.Second * 1)
	}
}

func createBroker(listener net.Listener, brokerID int) Broker {
	brokerAddr := listener.Addr().String()
	peers := Peers{all: make(map[string]*networking.Conn)}
	partitions := make([]*Partition, config.NumOfPartitions)
	for i, _ := range partitions {
		partitions[i] = &Partition{}
	}
	logger := govec.InitGoVector(strconv.Itoa(brokerID), strconv.Itoa(brokerID))
	return Broker{brokerID, brokerAddr, peers, partitions, logger}
}

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
	}
}

func handleErrorFatal(msg string, e error) {
	if e != nil {
		errLog.Fatalf("%s, err = %s\n", msg, e.Error())
	}
}
