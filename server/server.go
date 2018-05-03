package main

import (
	"../networking"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Server struct {
	allBrokers brokers
}

type brokers struct {
	sync.RWMutex
	all map[string]*networking.Conn
}

type NoBrokersAvailableError string

func (e NoBrokersAvailableError) Error() string {
	return fmt.Sprintf("[SERVER] No Brokers Currently Available")
}

func (s *Server) Register(newAddr *string, reply *int) error {
	newConn, err := rpc.Dial("tcp", *newAddr)
	checkError(err)
	defer newConn.Close()

	s.allBrokers.Lock()

	if _, exists := s.allBrokers.all[*newAddr]; exists {
		fmt.Println("reconnected to", *newAddr)
	}

	conn, err := networking.CreateConn(*newAddr)
	if err != nil {
		return err
	}
	s.allBrokers.all[*newAddr] = conn
	s.allBrokers.Unlock()

	*reply = 1
	fmt.Println("Registered new broker:", *newAddr)

	//tell all peers about the new non-denominational entity...
	s.distributeNewPeer(*newAddr)
	s.informNewPeer(newConn, *newAddr)
	return nil
}

func (s *Server) HeartBeat(addr *string, ignored *bool) error {
	s.allBrokers.Lock()
	conn, ok := s.allBrokers.all[*addr]
	if ok {
		networking.UpdateRecentHeartBeat(conn)
	}
	s.allBrokers.Unlock()
	return nil
}

func (s *Server) GetBrokerAddr(_ignored *bool, brokerAddr *string) error {
	s.allBrokers.Lock()
	defer s.allBrokers.Unlock()
	if len(s.allBrokers.all) > 0 {
		i := rand.Intn(len(s.allBrokers.all))
		for k := range s.allBrokers.all {
			if i == 0 {
				fmt.Println("Sending address to client:", k)
				*brokerAddr = k
			}
			i--
		}
	} else {
		return NoBrokersAvailableError("")
	}
	return nil
}

func (s *Server) monitorBrokers() {
	for {
		s.allBrokers.Lock()
		for addr, broker := range s.allBrokers.all {
			if !networking.CheckHeartBeat(broker) {
				fmt.Printf("%s timed out\n", addr)
				broker.Conn.Close()
				delete(s.allBrokers.all, addr)
			} else {
				fmt.Printf("%s is alive\n", addr)
			}
		}
		s.allBrokers.Unlock()
		time.Sleep(time.Second * 2)
	}
}

func (s *Server) distributeNewPeer(newAddr string) {
	for _, conn := range s.allBrokers.all {
		if conn.Addr != newAddr {
			reply := 0
			err := conn.Conn.Call("Broker.AddPeer", &newAddr, &reply)
			checkError(err)
			if reply == 1 {
				fmt.Println("Distributed ", newAddr, "to", conn.Addr)
			} else {
				fmt.Println("ERROR: Couldn't distribute", newAddr, "to", conn.Addr)
			}
		}
	}
}

func (s *Server) informNewPeer(newConn *rpc.Client, newAddr string) {
	for _, conn := range s.allBrokers.all {
		if conn.Addr != newAddr {
			reply := 0
			err := newConn.Call("Broker.AddPeer", &conn.Addr, &reply)
			checkError(err)
			if reply == 1 {
				fmt.Println("Told ", newAddr, "about", conn.GetAddr())
			} else {
				fmt.Println("ERR: Couldnt tell", newAddr, "about", conn.Addr)
			}
		}
	}
}

func main() {
	port := ":0"
	if len(os.Args) == 2 {
		// Optional Port number
		port = ":" + os.Args[1]
	}

	server := Server{brokers{all: make(map[string]*networking.Conn)}}

	// server's listener
	listener, err := net.Listen("tcp", networking.GetOutboundIP()+port)
	checkError(err)
	go rpc.Accept(listener)
	fmt.Println("Server is listening on :", listener.Addr().String())

	rpc.Register(&server)

	go server.monitorBrokers()

	time.Sleep(time.Hour * 1)
}

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
	}
}
