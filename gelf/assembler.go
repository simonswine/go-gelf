package gelf

import (
	"bytes"
	"time"
)

// Assembler provides GELF message de-chunking
type assembler struct {
	deadline   time.Time
	chunksData [][]byte
	processed  int
	totalBytes int
}

// NewAssembler returns empty Assembler with maximum message size and duration
// specified.
func newAssembler(timeout time.Duration) (res *assembler) {
	res = new(assembler)
	res.deadline = time.Now().Add(timeout)
	return
}

// Bytes returns message bytes, not nessesarily fully assembled
func (a *assembler) Bytes() []byte {
	return bytes.Join(a.chunksData, nil)
}

// Expired returns true if first chunk is too old
func (a *assembler) Expired() bool {
	return time.Now().After(a.deadline)
}

func (a *assembler) getProcessedCount() int {
	return a.processed
}

// Update feeds the byte chunk to Assembler, returns ok when the message is
// complete.
func (a *assembler) Update(chunk []byte) bool {
	num, count := int(chunk[10]), int(chunk[11])
	if a.chunksData == nil {
		a.chunksData = make([][]byte, count)
	}
	if count != len(a.chunksData) || num >= count {
		return false
	}
	body := chunk[12:]
	if a.chunksData[num] == nil {
		chunkData := make([]byte, 0, len(body))
		a.totalBytes += len(body)
		a.chunksData[num] = append(chunkData, body...)
		a.processed++
	}
	return a.processed == len(a.chunksData)
}
