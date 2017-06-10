package etude0

import (
	"fmt"
	"strings"
)

type Pointer struct {
	val int
}

func NewPointer(val int) *Pointer {
	return &Pointer{val}
}

func (p *Pointer) Val() int {
	return p.val
}

func (p *Pointer) Empty() bool {
	return p.val == 0
}

func (p *Pointer) String() string {
	n := fmt.Sprintf("%d", p.Val())
	return strings.Repeat("0", ptrByteSize-len(n)) + n
}
