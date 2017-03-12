package pgtype

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/jackc/pgx/pgio"
)

type Hstore struct {
	Map    map[string]Text
	Status Status
}

func (dst *Hstore) Set(src interface{}) error {
	switch value := src.(type) {
	case map[string]string:
		m := make(map[string]Text, len(value))
		for k, v := range value {
			m[k] = Text{String: v, Status: Present}
		}
		*dst = Hstore{Map: m, Status: Present}
	default:
		return fmt.Errorf("cannot convert %v to Tid", src)
	}

	return nil
}

func (dst *Hstore) Get() interface{} {
	switch dst.Status {
	case Present:
		return dst.Map
	case Null:
		return nil
	default:
		return dst.Status
	}
}

func (src *Hstore) AssignTo(dst interface{}) error {
	switch v := dst.(type) {
	case *map[string]string:
		switch src.Status {
		case Present:
			*v = make(map[string]string, len(src.Map))
			for k, val := range src.Map {
				if val.Status != Present {
					return fmt.Errorf("cannot decode %v into %T", src, dst)
				}
				(*v)[k] = val.String
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

func (dst *Hstore) DecodeText(src []byte) error {
	if src == nil {
		*dst = Hstore{Status: Null}
		return nil
	}

	return nil
}

func (dst *Hstore) DecodeBinary(src []byte) error {
	if src == nil {
		*dst = Hstore{Status: Null}
		return nil
	}

	rp := 0

	if len(src[rp:]) < 4 {
		return fmt.Errorf("hstore incomplete", src)
	}
	pairCount := int(int32(binary.BigEndian.Uint32(src[rp:])))
	rp += 4

	m := make(map[string]Text, pairCount)

	for i := 0; i < pairCount; i++ {
		if len(src[rp:]) < 4 {
			return fmt.Errorf("hstore incomplete", src)
		}
		keyLen := int(int32(binary.BigEndian.Uint32(src[rp:])))
		rp += 4

		if len(src[rp:]) < keyLen {
			return fmt.Errorf("hstore incomplete", src)
		}
		key := string(src[rp : rp+keyLen])
		rp += keyLen

		if len(src[rp:]) < 4 {
			return fmt.Errorf("hstore incomplete", src)
		}
		valueLen := int(int32(binary.BigEndian.Uint32(src[rp:])))
		rp += 4

		var valueBuf []byte
		if valueLen >= 0 {
			valueBuf = src[rp : rp+valueLen]
		}

		var value Text
		err := value.DecodeBinary(valueBuf)
		if err != nil {
			return err
		}
		m[key] = value
	}

	*dst = Hstore{Map: m, Status: Present}

	return nil
}

func (src Hstore) EncodeText(w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	err := error(nil)
	return false, err
}

func (src Hstore) EncodeBinary(w io.Writer) (bool, error) {
	switch src.Status {
	case Null:
		return true, nil
	case Undefined:
		return false, errUndefined
	}

	_, err := pgio.WriteInt32(w, int32(len(src.Map)))
	if err != nil {
		return false, err
	}

	elemBuf := &bytes.Buffer{}
	for k, v := range src.Map {
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
