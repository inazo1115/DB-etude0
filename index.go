package etude0

import (
	"fmt"
	"os"
)

type Index struct {
	idxFile   *os.File
	datFile   *os.File
	freeList  *FreeList
	hashTable *HashTable
	chains    map[string]*IndexRecordChain
}

func NewIndex(idxFile, datFile *os.File) (*Index, error) {

	freeList := NewFreeList(idxFile, datFile)

	hashTable, err := NewHashTable(idxFile)
	if err != nil {
		return nil, err
	}

	chains := make(map[string]*IndexRecordChain)

	return &Index{idxFile, datFile, freeList, hashTable, chains}, nil
}

func (idx *Index) Close() error {
	return nil
}

func (idx *Index) Get(key string) (string, error) {

	idx.hashTable.RLock()
	ptr, err := idx.hashTable.GetChainHead(key)
	idx.hashTable.RUnlock()
	if err != nil {
		return "", err
	}
	if ptr.Empty() {
		return "", fmt.Errorf("not found: " + key)
	}

	chain := idx.getChain(ptr)
	chain.RLock()
	defer chain.RUnlock()

	for chain.HasNext() {
		rec, err := chain.Next()
		if err != nil {
			return "", err
		}
		if rec.Key() == key {
			value, err := idx.readData(rec.DatOff(), rec.DatLen())
			if err != nil {
				return "", err
			}
			return value, nil
		}
	}

	return "", fmt.Errorf("not found: " + key)
}

func (idx *Index) Put(key string, value string) error {

	idx.hashTable.RLock()
	ptr, err := idx.hashTable.GetChainHead(key)
	idx.hashTable.RUnlock()
	if err != nil {
		return err
	}

	if ptr.Empty() {

		idx.hashTable.Lock()
		defer idx.hashTable.Unlock()

		newIdxPtr := idx.freeList.getFreeIdxPtr(key, value)
		newDatPtr := idx.freeList.getFreeDatPtr(key, value)
		newRec := NewIndexRecord(NewPointer(0), key, newDatPtr.Val(), len(value))

		chain := idx.getChain(newIdxPtr)
		chain.Lock()
		defer chain.Unlock()

		idx.hashTable.Update(key, newIdxPtr)
		chain.Append(newIdxPtr, newRec)
		idx.writeData(newDatPtr, value)

		return nil
	}

	chain := idx.getChain(ptr)
	chain.Lock()
	defer chain.Unlock()

	for chain.HasNext() {
		rec, err := chain.Next()
		if err != nil {
			return err
		}
		if rec.Key() == key {
			// TODO: logical delete
			return fmt.Errorf("already exists: " + rec.String())
		}
	}

	newIdxPtr := idx.freeList.getFreeIdxPtr(key, value)
	newDatPtr := idx.freeList.getFreeDatPtr(key, value)
	newRec := NewIndexRecord(NewPointer(0), key, newDatPtr.Val(), len(value))

	chain.Append(newIdxPtr, newRec)
	idx.writeData(newDatPtr, value)

	return nil
}

func (idx *Index) Optimize() error {
	return nil
}

func (idx *Index) Dump() {
}

func (idx *Index) getChain(ptr *Pointer) *IndexRecordChain {
	if chain := idx.chains[ptr.String()]; chain != nil {
		chain.Init()
		return chain
	}
	chain := NewIndexRecordChain(idx.idxFile, ptr)
	idx.chains[ptr.String()] = chain
	chain.Init()
	return chain
}

func (idx *Index) readData(off, len int) (string, error) {
	buf := make([]byte, len)
	if _, err := idx.datFile.ReadAt(buf, int64(off)); err != nil {
		return "", err
	}
	return string(buf[:]), nil
}

func (idx *Index) writeData(ptr *Pointer, value string) error {
	v := []byte(value + "\n")
	if _, err := idx.datFile.WriteAt(v, int64(ptr.Val())); err != nil {
		return err
	}
	return nil
}
