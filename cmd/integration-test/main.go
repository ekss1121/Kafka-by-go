package main

import (
	"bytes"
	"fmt"
	"goKafka/client"
	"io"
	"log"
	"strconv"
	"strings"
)

const maxN = 10000000
const maxBufferSize = 1024 * 1024

func send(s *client.Simple) (sum int64, err error) {
	var b bytes.Buffer
	sum = 0
	for i := 0; i<maxN; i++ {
		fmt.Fprintf(&b, "%d\n", i)
		sum += int64(i)
		if b.Len() >= maxBufferSize {
			if err := s.Send(b.Bytes()); err != nil {
				return 0, err
			}

			b.Reset()
		}
	}

	if b.Len() != 0 {
		if err := s.Send(b.Bytes()); err != nil {
			return 0, err
		}
	}
	return sum, nil
}

func receive(s *client.Simple) (sum int64, err error) {
	buf := make([]byte, maxBufferSize)
	for {
		res, err := s.Receive(buf)
		if err == io.EOF {
			return sum, nil
		} else if err != nil {
			return 0, err
		}

		ints := strings.Split(string(res), "\n")
		for _, str := range ints {
			if str == "" {
				continue
			}

			i, err := strconv.Atoi(str)
			if err != nil {
				return 0, err
			}

			sum += int64(i)
		}
	}
}

func main() {
	s := client.NewSimple([]string{"localhost"})
	want, err := send(s)
	if err != nil {
		log.Fatalf("Send error %v", err)
	}
	got, err := receive(s)

	if err != nil {
		log.Fatalf("Receive error %v", err)
	}

	if want != got {
		log.Fatalf("The expected sum %d is not align with the received sum %d", want , got)
	} else {
		log.Printf("The test has passed!\n")
	}
}