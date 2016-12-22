// Copyright 2016 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package readahead

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"sync"
	"testing"
	"time"
)

const srcSize int64 = 1E6

func numReads(chunkSize int) int {
	blocks := int(srcSize / int64(chunkSize))
	if int64(blocks*chunkSize) < srcSize {
		blocks++
	}
	blocks++ // EOF read at end
	return blocks
}

func TestReader(t *testing.T) {
	testData := generateTestData(srcSize)

	newReader := func(path string, r *countingReader, chunkSize, chunkAhead, numWorkers int) io.ReadCloser {
		return NewReader(path, r, chunkSize, chunkAhead)
	}
	newConcurrentReader := func(path string, r *countingReader, chunkSize, chunkAhead, numWorkers int) io.ReadCloser {
		return NewConcurrentReader(path, r, chunkSize, chunkAhead, numWorkers)
	}

	tests := []struct {
		newReader  func(path string, r *countingReader, chunkSize, chunkAhead, numWorkers int) io.ReadCloser
		chunkSize  int
		chunkAhead int
		numWorkers int
		stopEarly  bool
		wantReads  int
	}{
		{newReader, 1000, 1, 0, false, numReads(1000)},
		{newReader, 1001, 1, 0, false, numReads(1001)},
		{newReader, 1000, 1, 0, true, 1},
		{newConcurrentReader, 1000, 1, 1, false, numReads(1000)},
		{newConcurrentReader, 1000, 10, 1, false, numReads(1000)},
		{newConcurrentReader, 1000, 10, 2, false, numReads(1000)},
		{newConcurrentReader, 1000, 10, 10, false, numReads(1000)},
		{newConcurrentReader, 1001, 10, 10, false, numReads(1000)},
		{newConcurrentReader, 1000, 10, 2, true, 1},
	}
	for _, tt := range tests {
		t.Logf("%+v: starting", tt)

		cr := &countingReader{Reader: bytes.NewReader(testData)}

		start := cr.readCount
		r := tt.newReader("buffer", cr, tt.chunkSize, tt.chunkAhead, tt.numWorkers)

		if tt.stopEarly {
			// Read only one byte, then close the file.
			data := make([]byte, 1)
			if count, err := r.Read(data); count != 1 || err != nil {
				t.Errorf("%+v: failed to read from test file: %v", tt, err)
				continue
			}
			if err := r.Close(); err != nil {
				t.Errorf("%+v: failed to close test file: %v", tt, err)
				continue
			}
		} else {
			data, err := ioutil.ReadAll(r)
			r.Close()
			if err != nil {
				t.Errorf("%+v: failed to read from reader: %v", tt, err)
				continue
			}
			if len(data) != len(testData) {
				t.Errorf("%+v: read %d bytes, but testData has %d bytes", tt, len(data), len(testData))
				continue
			}
			if !bytes.Equal(data, testData) {
				t.Errorf("%+v: data != testData, but lengths match (%d bytes)", tt, len(testData))
				continue
			}
		}

		// Channel buffers allow up to chunkAhead+(2*numWorkers)+3 extra reads.
		wantReads := tt.wantReads + tt.chunkAhead + (2 * tt.numWorkers) + 3
		if reads := cr.readCount - start; reads > wantReads {
			t.Errorf("%+v: got %d reads, want <= %d reads", tt, reads, wantReads)
		}
	}
}

func generateTestData(length int64) []byte {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	data := make([]byte, length)
	for i := range data {
		data[i] = byte(rnd.Intn(256))
	}
	return data
}

type countingReader struct {
	sync.Mutex
	readCount int
	*bytes.Reader
}

func (c *countingReader) Read(p []byte) (n int, err error) {
	c.Lock()
	c.readCount++
	c.Unlock()

	time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
	return c.Reader.Read(p)
}
