package etude0

import (
	"fmt"
	"strconv"
	"strings"
)

type IndexRecord struct {
	chainPtr *Pointer
	idxLen   int
	key      string
	datOff   int
	datLen   int
}

func NewIndexRecord(chainPtr *Pointer, key string, datOff int, datLen int) *IndexRecord {
	idxLen := len(key) + 1 + len(fmt.Sprintf("%d", datOff)) + 1 + len(fmt.Sprintf("%d", datLen))
	return &IndexRecord{chainPtr, idxLen, key, datOff, datLen}
}

func FromByte(byt []byte) (*IndexRecord, error) {

	ret := &IndexRecord{}

	i, err := strconv.Atoi(string(byt[0:ptrByteSize]))
	if err != nil {
		return nil, err
	}
	ret.chainPtr = NewPointer(i)

	ret.idxLen, err = strconv.Atoi(string(byt[ptrByteSize:(ptrByteSize * 2)]))
	if err != nil {
		return nil, err
	}

	s := strings.Split(string(byt[(ptrByteSize*2):]), ":")
	ret.key = s[0]

	ret.datOff, err = strconv.Atoi(s[1])
	if err != nil {
		return nil, err
	}

	ret.datLen, err = strconv.Atoi(s[2])
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (rec *IndexRecord) ToByte() []byte {
	return []byte(rec.String())
}

func (rec *IndexRecord) Key() string {
	return rec.key
}

func (rec *IndexRecord) DatOff() int {
	return rec.datOff
}

func (rec *IndexRecord) DatLen() int {
	return rec.datLen
}

func (rec *IndexRecord) ChainPtr() *Pointer {
	return rec.chainPtr
}

func (rec *IndexRecord) String() string {
	ret := rec.chainPtr.String()
	l := fmt.Sprintf("%d", rec.idxLen)
	ret += strings.Repeat("0", ptrByteSize-len(l)) + l
	ret += rec.key
	ret += ":"
	ret += fmt.Sprintf("%d", rec.datOff)
	ret += ":"
	ret += fmt.Sprintf("%d", rec.datLen)
	return ret
}
