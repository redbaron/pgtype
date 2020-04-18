package pgtype_test

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgtype"

	pgbinary "github.com/jackc/pgtype/binary"
	pgx "github.com/jackc/pgx/v4"
	errors "golang.org/x/xerrors"
)

type MyType struct {
	a int32   // NULL will cause decoding error
	b *string // there can be NULL in this position in SQL
}

func (dst *MyType) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		return errors.New("NULL values can't be decoded. Scan into a &*MyType to handle NULLs")
	}

	a := pgtype.Int4{}
	b := pgtype.Text{}

	if err := pgtype.ScanRowValue(ci, src, &a, &b); err != nil {
		return err
	}

	// type compatibility is checked by AssignTo
	// only lossless assignments will succeed
	if err := a.AssignTo(&dst.a); err != nil {
		return err
	}

	// AssignTo also deals with null value handling
	if err := b.AssignTo(&dst.b); err != nil {
		return err
	}

	return nil
}

func (src MyType) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) (newBuf []byte, err error) {
	a := pgtype.Int4{src.a, pgtype.Present}
	var b pgtype.Text
	if src.b != nil {
		b = pgtype.Text{*src.b, pgtype.Present}
	} else {
		b = pgtype.Text{Status: pgtype.Null}
	}

	return pgtype.EncodeRow(ci, buf, &a, &b)
}

type MyTypeArray []MyType

type ArrayHeaderCb func(nullArray bool, elementCount int, dims []pgbinary.ArrayDimension, oid uint32) error
type ArrayElementCb func(isNull bool, pos int, ci *pgtype.ConnInfo, src []byte) error

func Array(alloc ArrayHeaderCb, decode ArrayElementCb) pgtype.BinaryDecoderFunc {
	return func(ci *pgtype.ConnInfo, src []byte) error {
		if src == nil {
			return alloc(true, 0, nil, 0)
		}

		it, elementCount, dims, oid, err := pgbinary.NewArrayIterator(src)
		if err != nil {
			return err
		}

		if err = alloc(false, elementCount, dims, oid); err != nil {
			return err
		}

		for i := 0; len(it) > 0; i++ {
			isNull, elemBytes, err := it.NextElem()
			if err != nil {
				return err
			}
			if err := decode(isNull, i, ci, elemBytes); err != nil {
				return err
			}
		}
		return nil
	}
}

func MyTypeArrayDecoder(dst *MyTypeArray) pgtype.BinaryDecoder {
	alloc := func(isNull bool, elementCount int, dims []pgbinary.ArrayDimension, oid uint32) error {
		if isNull {
			*dst = nil
		} else if len(*dst) < elementCount {
			*dst = append(*dst, make([]MyType, (elementCount-len(*dst)))...)
		}
		return nil
	}
	decode := func(isNull bool, i int, ci *pgtype.ConnInfo, elemBytes []byte) error {
		if isNull {
			return errors.Errorf("ARRAY's %d th element is NULL, decode into slice of pointers instead", i)
		}
		return (*dst)[i].DecodeBinary(ci, elemBytes)
	}
	return Array(alloc, decode)
}

func (dst *MyTypeArray) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	return MyTypeArrayDecoder(dst).DecodeBinary(ci, src)
}

func ptrS(s string) *string {
	return &s
}

func E(err error) {
	if err != nil {
		panic(err)
	}
}

// ExampleCustomCompositeTypes demonstrates how support for custom types mappable to SQL
// composites can be added.
func Example_customCompositeTypes() {
	conn, err := pgx.Connect(context.Background(), os.Getenv("PGX_TEST_DATABASE"))
	E(err)

	defer conn.Close(context.Background())
	_, err = conn.Exec(context.Background(), `drop type if exists mytype;

create type mytype as (
  a int4,
  b text
);`)
	E(err)
	defer conn.Exec(context.Background(), "drop type mytype")

	var result *MyType

	// Demonstrates both passing and reading back composite values
	err = conn.QueryRow(context.Background(), "select $1::mytype",
		pgx.QueryResultFormats{pgx.BinaryFormatCode}, MyType{1, ptrS("foo")}).
		Scan(&result)
	E(err)
	fmt.Printf("First row: a=%d b=%s\n", result.a, *result.b)

	var resSlice MyTypeArray = make([]MyType, 2)

	// Demonstrates reading back array of composites
	err = conn.QueryRow(context.Background(), "select ARRAY[$1::mytype, (99, 'zzz')::mytype]",
		pgx.QueryResultFormats{pgx.BinaryFormatCode}, MyType{1, ptrS("foo")}).
		Scan(&resSlice)
	E(err)

	fmt.Printf("Array: 1=%s 2=%s\n", *resSlice[0].b, *resSlice[1].b)

	// Because we scan into &*MyType, NULLs are handled generically by assigning nil to result
	err = conn.QueryRow(context.Background(), "select NULL::mytype", pgx.QueryResultFormats{pgx.BinaryFormatCode}).Scan(&result)
	E(err)

	fmt.Printf("Second row: %v\n", result)

	// Output:
	// First row: a=1 b=foo
	// Array: 1=foo 2=zzz
	// Second row: <nil>
}
