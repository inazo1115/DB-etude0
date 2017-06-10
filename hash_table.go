package etude0

import (
	"hash/fnv"
	"os"
	"strconv"
	"strings"
	"sync"
)

type HashTable struct {
	sync.RWMutex
	idxFile *os.File
}

func NewHashTable(idxFile *os.File) (*HashTable, error) {
	initTable(idxFile)
	return &HashTable{idxFile: idxFile}, nil
}

func (tbl *HashTable) GetChainHead(key string) (*Pointer, error) {

	off := getOffset(key)

	buf := make([]byte, ptrByteSize)
	if _, err := tbl.idxFile.ReadAt(buf, off); err != nil {
		return nil, err
	}

	i, err := strconv.Atoi(string(buf[:]))
	if err != nil {
		return nil, err
	}

	return NewPointer(i), nil
}

func (tbl *HashTable) Update(key string, ptr *Pointer) error {

	off := getOffset(key)

	buf := []byte(ptr.String())
	if _, err := tbl.idxFile.WriteAt(buf, off); err != nil {
		return err
	}

	return nil
}

func initTable(idxFile *os.File) error {

	fi, err := idxFile.Stat()
	if err != nil {
		return err
	}

	if fi.Size() >= ptrByteSize*(2+hashTableSize) {
		return nil
	}

	zeros := []byte(strings.Repeat("0", ptrByteSize*(1+hashTableSize)))
	if _, err := idxFile.WriteAt(zeros, 0); err != nil {
		return err
	}

	return nil
}

func getOffset(key string) int64 {
	n := hash(key) % hashTableSize
	off := int64(ptrByteSize + n*ptrByteSize)
	return off
}

func hash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}
