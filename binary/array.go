package binary

import (
	"encoding/binary"

	errors "golang.org/x/xerrors"
)

// Information on the internals of PostgreSQL arrays can be found in
// src/include/utils/array.h and src/backend/utils/adt/arrayfuncs.c. Of
// particular interest is the array_send function.

type arrayHeader struct {
	ContainsNull bool
	ElementOID   int32
	Dimensions   []ArrayDimension
}

type ArrayDimension struct {
	Length     int32
	LowerBound int32
}

type ArrayIterator []byte

func NewArrayIterator(src []byte) (ArrayIterator, int, []ArrayDimension, uint32, error) {
	if len(src) < 12 {
		return ArrayIterator{}, 0, nil, 0, errors.Errorf("array header too short: %d", len(src))
	}

	rp := 0

	numDims := int(binary.BigEndian.Uint32(src[rp:]))
	rp += 4

	//ContainesNull
	_ = binary.BigEndian.Uint32(src[rp:]) == 1
	rp += 4

	ElementOID := binary.BigEndian.Uint32(src[rp:])
	rp += 4

	if len(src) < 12+numDims*8 {
		return ArrayIterator{}, 0, nil, 0, errors.Errorf("array header too short for %d dimensions: %d", numDims, len(src))
	}

	var dims []ArrayDimension
	if numDims > 0 {
		dims = make([]ArrayDimension, numDims)
	}
	for i := range dims {
		dims[i].Length = int32(binary.BigEndian.Uint32(src[rp:]))
		rp += 4

		dims[i].LowerBound = int32(binary.BigEndian.Uint32(src[rp:]))
		rp += 4
	}

	elementCount := dims[0].Length
	for _, d := range dims[1:] {
		elementCount *= d.Length
	}

	return ArrayIterator(src[rp:]), int(elementCount), dims, ElementOID, nil
}

func (it *ArrayIterator) NextElem() (isNull bool, elemBytes []byte, err error) {
	if len(*it) < 4 {
		return false, nil, errors.Errorf("array element too short")
	}
	elemLen := int(int32(binary.BigEndian.Uint32((*it)[0:])))
	rp := 4

	if len(*it) < rp+elemLen {
		return false, nil, errors.Errorf("array element too short")
	}

	elemBytes = (*it)[rp : rp+elemLen]
	*it = (*it)[rp+elemLen:]
	return //TODO test nulls
}
