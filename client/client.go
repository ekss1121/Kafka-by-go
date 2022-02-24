package client

import (
	"bytes"
)

const defaultStrachSize = 64 * 1024

type Simple struct {
	addrs []string

	buf bytes.Buffer
}

func NewSimple(addrs []string) *Simple {
	return &Simple{
		addrs: addrs,
	}
}

// Send /** Send the messages to the Gokafka servers
func (s *Simple) Send(mmsgs []byte) error {
		_, err := s.buf.Write(mmsgs)
		return err
}

// Receive /** Receive the messages sent to GoKafka servers
func (s *Simple) Receive(strach []byte) ([]byte, error) {
	if strach == nil {
		strach = make([]byte, defaultStrachSize)
	}

	n, err := s.buf.Read(strach)
	if err != nil {
		return nil, err
	}

	truncated, rest := cutToLastMessage(strach[0:n])
	_ = rest

	return truncated, nil
}

func cutToLastMessage(res []byte) (truncated []byte, rest []byte) {
	n := len(res)
	if n == 0 {
		return res, nil
	}

	if res[n-1] == '\n' {
		return res, nil
	}

	lastPos := bytes.LastIndexByte(res, '\n')
	return res[0:lastPos+1], res[lastPos+1:]
}
