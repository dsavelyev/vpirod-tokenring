package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

type maintMsgType int

const (
	MSG_NONE = iota
	MSG_DATA
	MSG_ACK
)

const (
	MAINT_SEND maintMsgType = iota
	MAINT_DROP
)

type dataMessage struct {
	Typ  int    `json:"type"`
	From int    `json:"from"`
	To   int    `json:"to"`
	Data string `json:"data,omitempty"`
}

type maintMessage struct {
	Typ  maintMsgType `json:"type"`
	Dst  int          `json:"dst"`
	Data string       `json:"data"`
}

// Implementation of json.(Un)marshaler for the "enum"
func (ty *maintMsgType) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	s = strings.ToLower(s)
	switch s {
	case "send":
		*ty = MAINT_SEND
	case "drop":
		*ty = MAINT_DROP
	default:
		return fmt.Errorf("invalid msg type %s", s)
	}

	return nil
}

func (ty maintMsgType) MarshalJSON() ([]byte, error) {
	var s string

	switch ty {
	case MAINT_SEND:
		s = "send"
	case MAINT_DROP:
		s = "drop"
	default:
		return nil, fmt.Errorf("invalid msg type %d", ty)
	}

	return json.Marshal(s)
}

type sendMsg struct {
	dst  int
	data string
}

type msgQueue struct {
	data  []*sendMsg
	head  int
	tail  int
	count int
}

func makeQueue() *msgQueue {
	return &msgQueue{
		data:  make([]*sendMsg, 10, 10),
		head:  0,
		tail:  0,
		count: 0,
	}
}

func (q *msgQueue) push(msg *sendMsg) {
	if q.head == q.tail && q.count > 0 {
		newbuf := make([]*sendMsg, 2*q.count)
		copy(newbuf, q.data[q.head:])
		copy(newbuf[len(newbuf):], q.data[:q.head])

		q.data = newbuf
		q.head = 0
		q.tail = q.count
	}

	q.data[q.tail] = msg
	q.tail = (q.tail + 1) % len(q.data)
	q.count++
}

func (q *msgQueue) pop() *sendMsg {
	if q.count == 0 {
		return nil
	} else {
		ret := q.data[q.head]
		q.head++
		q.count--
		return ret
	}
}

func (q *msgQueue) peek() *sendMsg {
	if q.count == 0 {
		return nil
	} else {
		return q.data[q.head]
	}
}
