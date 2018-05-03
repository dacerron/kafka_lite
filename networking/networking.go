package networking

import (
	"fmt"
	"net"
	"net/rpc"
	"strings"
	"time"
)

type Conn struct {
	Conn            *rpc.Client
	Addr            string
	RecentHeartBeat int64
}

func GetOutboundIP() string {
	conn, err := net.DialTimeout("udp", "8.8.8.8:80", time.Second*2)
	checkError(err)
	defer conn.Close()
	localAddr := conn.LocalAddr().String()
	idx := strings.LastIndex(localAddr, ":")
	return localAddr[0:idx]
}

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
	}
}

func (c *Conn) GetAddr() string {
	return c.Addr
}

func CreateConn(addr string) (*Conn, error) {
	conn, err := rpc.Dial("tcp", addr)
	if err != nil {
		return &Conn{}, err
	}
	return &Conn{conn, addr, time.Now().UnixNano()}, nil
}
