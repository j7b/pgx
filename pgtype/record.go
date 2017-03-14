package pgtype

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/jackc/pgx/pgio"
)

// Record is the generic PostgreSQL record type such as is created with the
// "row" function. Only the binary format is implemented as the text format
// lacks field type information (PostgreSQL doesn't even implenent text input
// for records).
type Record struct {
	Fields []Value
	Status Status
}

func (dst *Record) Set(src interface{}) error {
	switch value := src.(type) {
	case []Value:
		*dst = Record{Fields: value, Status: Present}
	default:
		return fmt.Errorf("cannot convert %v to Record", src)
	}

	return nil
}

func (dst *Record) Get() interface{} {
	switch dst.Status {
	case Present:
		return dst.Fields
	case Null:
		return nil
	default:
		return dst.Status
	}
}

func (src *Record) AssignTo(dst interface{}) error {
	switch v := dst.(type) {
	case *[]Value:
		switch src.Status {
		case Present:
			*v = make([]Value, len(src.Fields))
			copy(*v, src.Fields)
		case Null:
			*v = nil
		default:
			return fmt.Errorf("cannot decode %v into %T", src, dst)
		}
	case *[]interface{}:
		switch src.Status {
		case Present:
			*v = make([]interface{}, len(src.Fields))
			for i := range *v {
				(*v)[i] = src.Fields[i].Get()
			}
		case Null:
			*v = nil
		default:
			return fmt.Errorf("cannot decode %v into %T", src, dst)
		}
	default:
		return fmt.Errorf("cannot decode %v into %T", src, dst)
	}

	return nil
}

func (dst *Record) DecodeBinary(src []byte) error {
	if src == nil {
		*dst = Record{Status: Null}
		return nil
	}

	type binaryDecoderValue interface {
		BinaryDecoder
		Value
	}

	// TODO - need to standardize where name/oid/pgtype mapping is handled.
	// Especially for types that have oids that can vary like hstore. To support
	// simultaneous connections to different PG servers it would need to be
	// parametized somehow. The below is simply a placeholder until that is
	// resolved.
	oidValues = map[uint32]func() binaryDecoderValue{
		AclitemArrayOid:     func() { return &AclitemArray{} },
		AclitemOid:          func() { return &Aclitem{} },
		BoolArrayOid:        func() { return &BoolArray{} },
		BoolOid:             func() { return &Bool{} },
		ByteaArrayOid:       func() { return &ByteaArray{} },
		ByteaOid:            func() { return &Bytea{} },
		CharOid:             func() { return &QChar{} },
		CidOid:              func() { return &Cid{} },
		CidrArrayOid:        func() { return &CidrArray{} },
		CidrOid:             func() { return &Inet{} },
		DateArrayOid:        func() { return &DateArray{} },
		DateOid:             func() { return &Date{} },
		Float4ArrayOid:      func() { return &Float4Array{} },
		Float4Oid:           func() { return &Float4{} },
		Float8ArrayOid:      func() { return &Float8Array{} },
		Float8Oid:           func() { return &Float8{} },
		InetArrayOid:        func() { return &InetArray{} },
		InetOid:             func() { return &Inet{} },
		Int2ArrayOid:        func() { return &Int2Array{} },
		Int2Oid:             func() { return &Int2{} },
		Int4ArrayOid:        func() { return &Int4Array{} },
		Int4Oid:             func() { return &Int4{} },
		Int8ArrayOid:        func() { return &Int8Array{} },
		Int8Oid:             func() { return &Int8{} },
		JsonbOid:            func() { return &Jsonb{} },
		JsonOid:             func() { return &Json{} },
		NameOid:             func() { return &Name{} },
		OidOid:              func() { return &Oid{} },
		TextArrayOid:        func() { return &TextArray{} },
		TextOid:             func() { return &Text{} },
		TidOid:              func() { return &Tid{} },
		TimestampArrayOid:   func() { return &TimestampArray{} },
		TimestampOid:        func() { return &Timestamp{} },
		TimestampTzArrayOid: func() { return &TimestamptzArray{} },
		TimestampTzOid:      func() { return &Timestamptz{} },
		VarcharArrayOid:     func() { return &VarcharArray{} },
		VarcharOid:          func() { return &Text{} },
		XidOid:              func() { return &Xid{} },
	}

	rp := 0

	if len(src[rp:]) < 4 {
		return fmt.Errorf("Record incomplete %v", src)
	}
	fieldCount := int(int32(binary.BigEndian.Uint32(src[rp:])))
	rp += 4

	fields := make([]Value, fieldCount)

	for i := 0; i < fieldCount; i++ {
		if len(src[rp:]) < 8 {
			return fmt.Errorf("Record incomplete %v", src)
		}
		fieldOid := binary.BigEndian.Uint32(src[rp:])
		rp += 4

		fieldLen := int(int32(binary.BigEndian.Uint32(src[rp:])))
		rp += 4

		if len(src[rp:]) < fieldLen {
			return fmt.Errorf("Record incomplete %v", src)
		}
		fieldBytes := src[rp : rp+fieldLen]
		rp += keyLen

		if len(src[rp:]) < 4 {
			return fmt.Errorf("Record incomplete %v", src)
		}
		valueLen := int(int32(binary.BigEndian.Uint32(src[rp:])))
		rp += 4

		var valueBuf []byte
		if valueLen >= 0 {
			if len(src[rp:]) < valueLen {
				return fmt.Errorf("Record incomplete %v", src)
			}
			valueBuf = src[rp : rp+valueLen]
			rp += valueLen
		}

		value := oidValues[fieldOid]()
		err := value.DecodeBinary(valueBuf)
		if err != nil {
			return err
		}
		fields[i] = value
	}

	*dst = Record{Fields: fields, Status: Present}

	return nil
}

func (src Record) EncodeBinary(w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := pgio.WriteInt32(w, int32(len(src.Fields)))
	if err != nil {
		return false, err
	}

	elemBuf := &bytes.Buffer{}
	for _, field := range src.Fields {
		_, err := pgio.WriteInt32(w, int32(len(k)))
		if err != nil {
			return false, err
		}
		_, err = io.WriteString(w, k)
		if err != nil {
			return false, err
		}

		null, err := v.EncodeText(elemBuf)
		if err != nil {
			return false, err
		}
		if null {
			_, err := pgio.WriteInt32(w, -1)
			if err != nil {
				return false, err
			}
		} else {
			_, err := pgio.WriteInt32(w, int32(elemBuf.Len()))
			if err != nil {
				return false, err
			}
			_, err = elemBuf.WriteTo(w)
			if err != nil {
				return false, err
			}
		}
	}

	return false, err
}
