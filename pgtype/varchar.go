package pgtype

import (
	"io"
)

type Varchar Text

// Set converts from src to dst. Note that as Varchar is not a general
// number type Set does not do automatic type conversion as other number
// types do.
func (dst *Varchar) Set(src interface{}) error {
	return (*Text)(dst).Set(src)
}

func (dst *Varchar) Get() interface{} {
	return (*Text)(dst).Get()
}

// AssignTo assigns from src to dst. Note that as Varchar is not a general number
// type AssignTo does not do automatic type conversion as other number types do.
func (src *Varchar) AssignTo(dst interface{}) error {
	return (*Text)(src).AssignTo(dst)
}

func (dst *Varchar) DecodeText(src []byte) error {
	return (*Text)(dst).DecodeText(src)
}

func (dst *Varchar) DecodeBinary(src []byte) error {
	return (*Text)(dst).DecodeBinary(src)
}

func (src Varchar) EncodeText(w io.Writer) (bool, error) {
	return (Text)(src).EncodeText(w)
}

func (src Varchar) EncodeBinary(w io.Writer) (bool, error) {
	return (Text)(src).EncodeBinary(w)
}
