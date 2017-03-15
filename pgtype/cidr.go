package pgtype

import (
	"io"
)

type Cidr Inet

func (dst *Cidr) Set(src interface{}) error {
	return (*Inet)(dst).Set(src)
}

func (dst *Cidr) Get() interface{} {
	return (*Inet)(dst).Get()
}

func (src *Cidr) AssignTo(dst interface{}) error {
	return (*Inet)(src).AssignTo(dst)
}

func (dst *Cidr) DecodeText(src []byte) error {
	return (*Inet)(dst).DecodeText(src)
}

func (dst *Cidr) DecodeBinary(src []byte) error {
	return (*Inet)(dst).DecodeBinary(src)
}

func (src Cidr) EncodeText(w io.Writer) (bool, error) {
	return (Inet)(src).EncodeText(w)
}

func (src Cidr) EncodeBinary(w io.Writer) (bool, error) {
	return (Inet)(src).EncodeBinary(w)
}
