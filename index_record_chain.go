package etude0

import (
	"os"
	"strconv"
	"sync"
)

type IndexRecordChain struct {
	sync.RWMutex
	idxFile *os.File
	init    *Pointer
	cursor  *Pointer
	last    *Pointer
}

func NewIndexRecordChain(idxFile *os.File, ptr *Pointer) *IndexRecordChain {
	return &IndexRecordChain{idxFile: idxFile, init: ptr, cursor: ptr, last: nil}
}

func (chain *IndexRecordChain) Init() {
	chain.cursor = chain.init
	chain.last = nil
}

func (chain *IndexRecordChain) Next() (*IndexRecord, error) {

	// Read idxLen first
	buf := make([]byte, ptrByteSize)
	if _, err := chain.idxFile.ReadAt(buf, int64(chain.cursor.Val()+ptrByteSize)); err != nil {
		return nil, err
	}
	idxLen, err := strconv.Atoi(string(buf[:]))
	if err != nil {
		return nil, err
	}

	// Read IndexRecord
	buf = make([]byte, idxLen+(2*ptrByteSize))
	if _, err := chain.idxFile.ReadAt(buf, int64(chain.cursor.Val())); err != nil {
		return nil, err
	}
	rec, err := FromByte(buf)
	if err != nil {
		return nil, err
	}

	// Update ptr
	chain.last = chain.cursor
	chain.cursor = rec.ChainPtr()

	return rec, nil
}

func (chain *IndexRecordChain) HasNext() bool {
	return !chain.cursor.Empty()
}

func (chain *IndexRecordChain) Append(ptr *Pointer, rec *IndexRecord) error {

	// Write new KeyValue
	if _, err := chain.idxFile.WriteAt(rec.ToByte(), int64(ptr.Val())); err != nil {
		return err
	}

	// Connect ptr
	if chain.last == nil {
		return nil
	}
	buf := []byte(ptr.String())
	if _, err := chain.idxFile.WriteAt(buf, int64(chain.last.Val())); err != nil {
		return err
	}

	return nil
}
