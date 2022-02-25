package client

import (
	"bytes"
	"errors"
)

const defaultStrachSize = 64 * 1024
var bufferTooSmall = errors.New("Buffer is too small!")

type Simple struct {
	addrs []string

	buf bytes.Buffer
	bufRest bytes.Buffer
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
func (s *Simple) Receive(scratch []byte) ([]byte, error) {
	if scratch == nil {
		scratch = make([]byte, defaultStrachSize)
	}
	// offset of the rest buffer size
	offset := 0
	if s.bufRest.Len() > 0 {
		if s.bufRest.Len() > len(scratch) {
			return nil, bufferTooSmall
		}

		n, err := s.bufRest.Read(scratch)
		if err != nil {
			return nil, err
		}
		s.bufRest.Reset()
		offset += n
	}

	n, err := s.buf.Read(scratch[offset:])
	if err != nil {
		return nil, err
	}

	truncated, rest, err := cutToLastMessage(scratch[0:n+offset])
	if err != nil {
		return nil, err
	}

	s.bufRest.Reset()
	s.bufRest.Write(rest)

	return truncated, nil
}

func cutToLastMessage(res []byte) (truncated []byte, rest []byte, err error) {
	n := len(res)
	if n == 0 {
		return res, nil,  nil
	}

	if res[n-1] == '\n' {
		return res, nil, nil
	}

	lastPos := bytes.LastIndexByte(res, '\n')
	if lastPos < 0 {
		return nil, nil, bufferTooSmall
	}
	return res[0:lastPos+1], res[lastPos+1:], nil
}
