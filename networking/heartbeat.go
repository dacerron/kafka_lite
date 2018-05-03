package networking

import (
	"net/rpc"
	"time"
)

// could make hearbeat time configurable, perhaps HeartBeatSettings?

func BrokerHeartBeat(broker *Conn, addr *string) error {
	var _ignored bool = false
	var address string = *addr
	err := broker.Conn.Call("Broker.HeartBeat", &address, &_ignored)
	return err
}

func ServerHeartBeat(server *rpc.Client, addr *string) error {
	var _ignored bool = false
	var address string = *addr
	err := server.Call("Server.HeartBeat", &address, &_ignored)
	return err
}

//Returns true if hearbeat is still good (less than two seconds since last)
func CheckHeartBeat(conn *Conn) bool {
	if (int64(time.Now().UnixNano()) - conn.RecentHeartBeat) > int64(time.Second * 2) {
		return false
	}
	return true
}

func UpdateRecentHeartBeat(conn *Conn) {
	conn.RecentHeartBeat = time.Now().UnixNano()
}
