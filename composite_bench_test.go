package pgtype_test

import (
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgtype/binary"
	pgbinary "github.com/jackc/pgtype/binary"
	errors "golang.org/x/xerrors"
)

type MyCompositeRaw struct {
	a int32
	b *string
}

func (src MyCompositeRaw) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) (newBuf []byte, err error) {
	a := pgtype.Int4{src.a, pgtype.Present}

	fieldBytes := make([]byte, 0, 64)
	fieldBytes, _ = a.EncodeBinary(ci, fieldBytes[:0])

	newBuf = binary.RecordStart(buf, 2)
	newBuf = binary.RecordAdd(newBuf, pgtype.Int4OID, fieldBytes)

	if src.b != nil {
		fieldBytes, _ = pgtype.Text{*src.b, pgtype.Present}.EncodeBinary(ci, fieldBytes[:0])
		newBuf = binary.RecordAdd(newBuf, pgtype.TextOID, fieldBytes)
	} else {
		newBuf = binary.RecordAddNull(newBuf, pgtype.TextOID)
	}
	return
}

func (dst *MyCompositeRaw) DecodeBinary(ci *pgtype.ConnInfo, src []byte) (err error) {
	return nil
}

type MyCompositeRawArray []MyCompositeRaw

func (dst *MyCompositeRawArray) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		*dst = nil
		return nil
	}

	it, elementCount, _, _, err := pgbinary.NewArrayIterator(src)
	if err != nil {
		return err
	}

	if len(*dst) < elementCount {
		*dst = append(*dst, make(MyCompositeRawArray, (elementCount-len(*dst)))...)
	}

	for i := 0; len(it) > 0; i++ {
		isNull, elemBytes, err := it.NextElem()
		if err != nil {
			return err
		}
		if isNull {
			return errors.Errorf("ARRAY's %d th element is NULL, decode into slice of pointers instead", i)
		}
		if err = (*dst)[i].DecodeBinary(ci, elemBytes); err != nil {
			return err
		}
	}
	return nil
}

var x []byte

func BenchmarkBinaryEncodingManual(b *testing.B) {
	buf := make([]byte, 0, 128)
	ci := pgtype.NewConnInfo()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		v := MyCompositeRaw{4, ptrS("ABCDEFG")}
		buf, _ = v.EncodeBinary(ci, buf[:0])
	}
	x = buf
}

func BenchmarkBinaryEncodingHelper(b *testing.B) {
	buf := make([]byte, 0, 128)
	ci := pgtype.NewConnInfo()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		v := MyType{4, ptrS("ABCDEFG")}
		buf, _ = v.EncodeBinary(ci, buf[:0])
	}
	x = buf
}

func BenchmarkBinaryEncodingRow(b *testing.B) {
	buf := make([]byte, 0, 128)
	ci := pgtype.NewConnInfo()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		c := pgtype.Composite(&pgtype.Int4{}, &pgtype.Text{})
		c.Set(pgtype.Row(2, "bar"))
		buf, _ = c.EncodeBinary(ci, buf[:0])
	}
	x = buf
}
