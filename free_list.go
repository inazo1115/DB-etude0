package etude0

import (
	"os"
)

type FreeList struct {
	idxFile *os.File
	datFile *os.File
}

func NewFreeList(idxFile, datFile *os.File) *FreeList {
	return &FreeList{idxFile, datFile}
}

// TODO: fix
func (free *FreeList) getFreeIdxPtr(key, value string) *Pointer {
	fi, _ := free.idxFile.Stat()
	return NewPointer(int(fi.Size()))
}

// TODO: fix
func (free *FreeList) getFreeDatPtr(key, value string) *Pointer {
	fi, _ := free.datFile.Stat()
	return NewPointer(int(fi.Size()))
}
