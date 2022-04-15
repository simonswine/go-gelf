// Copyright 2012 SocialCode. All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.

package gelf

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	maxMessageTimeout         = 5 * time.Second
	assemblersCleanUpInterval = 5 * time.Second
)

type Reader struct {
	mu              sync.Mutex
	conn            net.Conn
	chunkAssemblers map[string]*assembler
	done            chan struct{}
}

func NewReader(addr string) (*Reader, error) {
	var err error
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("ResolveUDPAddr('%s'): %s", addr, err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("ListenUDP: %s", err)
	}

	r := new(Reader)
	r.conn = conn
	r.chunkAssemblers = make(map[string]*assembler)
	r.done = make(chan struct{})
	r.initAssemblersCleanup()
	return r, nil
}

func (r *Reader) Addr() string {
	return r.conn.LocalAddr().String()
}

// FIXME: this will discard data if p isn't big enough to hold the
// full message.
func (r *Reader) Read(p []byte) (int, error) {
	msg, err := r.ReadMessage()
	if err != nil {
		return -1, err
	}

	var data string

	if msg.Full == "" {
		data = msg.Short
	} else {
		data = msg.Full
	}

	return strings.NewReader(data).Read(p)
}

func (r *Reader) ReadMessage() (*Message, error) {
	cBuf := make([]byte, ChunkSize)
	var (
		err       error
		n         int
		chunkHead []byte
		cReader   io.Reader
	)
	var message []byte

	for {
		if n, err = r.conn.Read(cBuf); err != nil {
			return nil, fmt.Errorf("Read: %s", err)
		}
		chunkHead, cBuf = cBuf[:2], cBuf[:n]
		if bytes.Equal(chunkHead, magicChunked) {
			messageID := string(cBuf[2 : 2+8])
			assembler := getAssembler(r, messageID)
			if assembler.processed >= MaxChunksCount {
				return nil, fmt.Errorf("too many chunks count for messageID: %v (max count %v)", messageID, MaxChunksCount)
			}
			if assembler.Expired() {
				return nil, fmt.Errorf("message with ID: %v is expired", messageID)
			}
			if messageCompleted := assembler.Update(cBuf); !messageCompleted {
				continue
			}
			message = assembler.Bytes()
			chunkHead = message[:2]
			delete(r.chunkAssemblers, messageID)
		} else {
			message = cBuf
		}
		break
	}

	// the data we get from the wire is compressed
	if bytes.Equal(chunkHead, magicGzip) {
		cReader, err = gzip.NewReader(bytes.NewReader(message))
	} else if chunkHead[0] == magicZlib[0] &&
		(int(chunkHead[0])*256+int(chunkHead[1]))%31 == 0 {
		// zlib is slightly more complicated, but correct
		cReader, err = zlib.NewReader(bytes.NewReader(message))
	} else {
		// compliance with https://github.com/Graylog2/graylog2-server
		// treating all messages as uncompressed if  they are not gzip, zlib or
		// chunked
		cReader = bytes.NewReader(message)
	}

	if err != nil {
		return nil, fmt.Errorf("NewReader: %s", err)
	}

	msg := new(Message)
	if err := json.NewDecoder(cReader).Decode(&msg); err != nil {
		return nil, fmt.Errorf("json.Unmarshal: %s", err)
	}

	return msg, nil
}

func getAssembler(r *Reader, messageID string) *assembler {
	r.mu.Lock()
	defer r.mu.Unlock()
	assembler, ok := r.chunkAssemblers[messageID]
	if !ok {
		assembler = newAssembler(maxMessageTimeout)
		r.chunkAssemblers[messageID] = assembler
	}
	return assembler
}

func (r *Reader) Close() error {
	close(r.done)
	return r.conn.Close()
}

func (r *Reader) initAssemblersCleanup() {
	go func() {
		for {
			select {
			case <-r.done:
				return
			case <-time.After(assemblersCleanUpInterval):
				cleanUpExpiredAssemblers(r)
			}
		}
	}()
}

func cleanUpExpiredAssemblers(r *Reader) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for messageID, assembler := range r.chunkAssemblers {
		if assembler.Expired() {
			delete(r.chunkAssemblers, messageID)
		}
	}
}
