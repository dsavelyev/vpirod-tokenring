package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

const (
	BASEPORT      = 30000
	BASEMAINTPORT = 40000

	SENDRECV_TIMEOUT = 100 * time.Millisecond
)

type nodeState struct {
	rank int

	dropNext bool

	// messages to send, they stay in the queue until acked
	toSend *msgQueue

	dataconn  *net.UDPConn
	maintconn *net.UDPConn

	nextAddr *net.UDPAddr
}

type ringParams struct {
	num int

	ip       net.IP
	interval float64
}

func getAddr(ip net.IP, baseport int, rank int) *net.UDPAddr {
	return &net.UDPAddr{
		IP:   ip,
		Port: baseport + rank,
		Zone: "",
	}
}

func receive(conn *net.UDPConn, msgs chan []byte, stopRecvCh chan struct{}) {
	messagebytes := make([]byte, 1024)

	for {
		select {
		case <-stopRecvCh:
			return
		default:
		}

		err := conn.SetReadDeadline(time.Now().Add(SENDRECV_TIMEOUT))
		if err != nil {
			panic(err)
		}

		nbytes, err := conn.Read(messagebytes)
		if err != nil {
			if e, ok := err.(net.Error); !ok || !e.Timeout() {
				panic(err)
			} else {
				continue
			}
		}

		select {
		case msgs <- messagebytes[:nbytes]: // empty
		case <-stopRecvCh:
			return
		}
	}
}

func handleMaintMsg(ring *ringParams, node *nodeState, msg *maintMessage) {
	switch msg.Typ {
	case MAINT_SEND:
		node.toSend.push(&sendMsg{
			dst:  msg.Dst,
			data: msg.Data,
		})
	case MAINT_DROP:
		node.dropNext = true
	}
}

func sendToNext(node *nodeState, body []byte) {
	err := node.dataconn.SetWriteDeadline(time.Now().Add(SENDRECV_TIMEOUT))
	if err != nil {
		panic(err)
	}

	_, err = node.dataconn.WriteToUDP(body, node.nextAddr)
	if err != nil {
		if e, ok := err.(net.Error); !ok || !e.Timeout() {
			panic(err)
		}
	}
}

func msgFromQueue(node *nodeState) dataMessage {
	var toSend dataMessage
	toSend.From = node.rank

	if newMsg := node.toSend.peek(); newMsg != nil {
		toSend.Typ = MSG_DATA
		toSend.To = newMsg.dst
		toSend.Data = newMsg.data
	} else {
		toSend.Typ = MSG_NONE
	}

	return toSend
}

func handleDataMsg(ring *ringParams, node *nodeState, msg *dataMessage) {
	if node.dropNext {
		node.dropNext = false

		fmt.Printf("node %d: dropping token\n", node.rank)
		return
	}

	var toSend dataMessage

	prevNode := ((node.rank-1)%ring.num + ring.num) % ring.num
	nextNode := (node.rank + 1) % ring.num

	switch msg.Typ {
	case MSG_NONE:
		// if we get a MSG_NONE after sending a message, we assume the token was
		// lost, and (re)send that message
		toSend = msgFromQueue(node)

		fmt.Printf("node %d: received token from node %d, sending token to node %d\n",
			node.rank, prevNode, nextNode)

	case MSG_DATA:
		if msg.To == node.rank {
			toSend.Typ = MSG_ACK
			toSend.To = msg.From
			toSend.From = node.rank
		} else {
			toSend.Typ = MSG_DATA
			toSend.From = msg.From
			toSend.To = msg.To
			toSend.Data = msg.Data
		}

		fmt.Printf("node %d: received token from node %d with data from node %d "+
			"(data='%s'), sending token to node %d\n",
			node.rank, prevNode, msg.From, msg.Data, nextNode)

	case MSG_ACK:
		if msg.To == node.rank {
			node.toSend.pop()
			toSend = msgFromQueue(node)
		} else {
			toSend.Typ = MSG_ACK
			toSend.From = msg.From
			toSend.To = msg.To
		}

		fmt.Printf("node %d: received token from node %d with delivery confirmation "+
			"from node %d, sending token to node %d\n",
			node.rank, prevNode, msg.From, nextNode)
	}

	body, err := json.Marshal(toSend)
	if err != nil {
		panic(err)
	}

	time.Sleep(time.Duration(ring.interval * float64(time.Millisecond)))
	sendToNext(node, body)
}

func sendNewToken(ring *ringParams, node *nodeState) {
	// we assume the token was lost, so if we have a message in the queue,
	// (re)send it now
	fmt.Printf("node %d: sending new token\n", node.rank)

	newToken := msgFromQueue(node)

	bytes, err := json.Marshal(newToken)
	if err != nil {
		panic(err)
	}

	sendToNext(node, bytes)
}

func initNode(ring *ringParams, rank int) *nodeState {
	var node nodeState
	node.rank = rank
	node.toSend = makeQueue()
	var err error

	node.dataconn, err = net.ListenUDP("udp4", getAddr(ring.ip, BASEPORT, rank))
	if err != nil {
		panic(err)
	}
	node.maintconn, err = net.ListenUDP("udp4", getAddr(ring.ip, BASEMAINTPORT, rank))
	if err != nil {
		panic(err)
	}

	node.nextAddr = getAddr(ring.ip, BASEPORT, (rank+1)%ring.num)

	return &node
}

func runNode(ring *ringParams, rank int) {
	node := initNode(ring, rank)

	datamsgs := make(chan []byte)
	maintmsgs := make(chan []byte)
	stopRecvData := make(chan struct{})
	stopRecvMaint := make(chan struct{})

	go receive(node.dataconn, datamsgs, stopRecvData)
	go receive(node.maintconn, maintmsgs, stopRecvMaint)

	defer func() {
		stopRecvData <- struct{}{}
		stopRecvMaint <- struct{}{}

		if err := node.dataconn.Close(); err == nil {
			return
		} else if err := node.maintconn.Close(); err == nil {
			return
		} else {
			panic(err)
		}
	}()

	tokenTimeout := time.Duration(float64(2*ring.num) * ring.interval * float64(time.Millisecond))

	var timeout *time.Timer
	var timeoutCh <-chan time.Time
	if node.rank == 0 {
		sendNewToken(ring, node)

		timeout = time.NewTimer(tokenTimeout)
		timeoutCh = timeout.C

		defer timeout.Stop()
	}

	for {
		select {
		case maintBytes := <-maintmsgs:
			{
				fmt.Printf("node %d: received service message: %s\n", node.rank, string(maintBytes))

				var msg maintMessage
				if err := json.Unmarshal(maintBytes, &msg); err != nil {
					panic(err)
				}

				handleMaintMsg(ring, node, &msg)
			}

		case dataBytes := <-datamsgs:
			{
				var msg dataMessage
				if err := json.Unmarshal(dataBytes, &msg); err != nil {
					panic(err)
				}

				handleDataMsg(ring, node, &msg)

				if timeout != nil {
					if !timeout.Stop() {
						<-timeoutCh
					}
					timeout.Reset(tokenTimeout)
				}
			}

		// blocks forever (because nil) unless we're 0
		case <-timeoutCh:
			sendNewToken(ring, node)
			timeout.Reset(tokenTimeout)
		}
	}
}

func parseArgs(n *int, t *float64) bool {
	index := 1

	gotN := false
	gotT := false

	for index < len(os.Args) {
		switch os.Args[index] {
		case "--n":
			index++
			val_n, err := strconv.Atoi(os.Args[index])
			if err != nil {
				return false
			}

			*n = val_n
			gotN = true

		case "--t":
			index++
			val_t, err := strconv.ParseFloat(os.Args[index], 64)
			if err != nil {
				return false
			}

			*t = val_t
			gotT = true

		default:
			return false
		}

		index++
	}

	return gotN && gotT
}

func main() {
	var params ringParams
	if !parseArgs(&params.num, &params.interval) {
		fmt.Printf("Usage: %s --n <int> --t <float>\n", os.Args[0])
		os.Exit(1)
	}

	params.ip = net.IPv4(127, 42, 42, 42)

	for i := 0; i < params.num; i++ {
		go runNode(&params, i)
	}

	// block forever
	select {}
}
