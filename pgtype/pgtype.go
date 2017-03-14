package pgtype

import (
	"errors"
	"io"
	"reflect"
)

// PostgreSQL oids for common types
const (
	BoolOid             = 16
	ByteaOid            = 17
	CharOid             = 18
	NameOid             = 19
	Int8Oid             = 20
	Int2Oid             = 21
	Int4Oid             = 23
	TextOid             = 25
	OidOid              = 26
	TidOid              = 27
	XidOid              = 28
	CidOid              = 29
	JsonOid             = 114
	CidrOid             = 650
	CidrArrayOid        = 651
	Float4Oid           = 700
	Float8Oid           = 701
	UnknownOid          = 705
	InetOid             = 869
	BoolArrayOid        = 1000
	Int2ArrayOid        = 1005
	Int4ArrayOid        = 1007
	TextArrayOid        = 1009
	ByteaArrayOid       = 1001
	VarcharArrayOid     = 1015
	Int8ArrayOid        = 1016
	Float4ArrayOid      = 1021
	Float8ArrayOid      = 1022
	AclitemOid          = 1033
	AclitemArrayOid     = 1034
	InetArrayOid        = 1041
	VarcharOid          = 1043
	DateOid             = 1082
	TimestampOid        = 1114
	TimestampArrayOid   = 1115
	DateArrayOid        = 1182
	TimestamptzOid      = 1184
	TimestamptzArrayOid = 1185
	RecordOid           = 2249
	UuidOid             = 2950
	JsonbOid            = 3802
)

type Status byte

const (
	Undefined Status = iota
	Null
	Present
)

type InfinityModifier int8

const (
	Infinity         InfinityModifier = 1
	None             InfinityModifier = 0
	NegativeInfinity InfinityModifier = -Infinity
)

type Value interface {
	// Set converts and assigns src to itself.
	Set(src interface{}) error

	// Get returns the simplest representation of Value. If the Value is Null or
	// Undefined that is the return value. If no simpler representation is
	// possible, then Get() returns Value.
	Get() interface{}

	// AssignTo converts and assigns the Value to dst.
	AssignTo(dst interface{}) error
}

type BinaryDecoder interface {
	// DecodeBinary decodes src into BinaryDecoder. If src is nil then the
	// original SQL value is NULL. BinaryDecoder MUST not retain a reference to
	// src. It MUST make a copy if it needs to retain the raw bytes.
	DecodeBinary(src []byte) error
}

type TextDecoder interface {
	// DecodeText decodes src into TextDecoder. If src is nil then the original
	// SQL value is NULL. TextDecoder MUST not retain a reference to src. It MUST
	// make a copy if it needs to retain the raw bytes.
	DecodeText(src []byte) error
}

// BinaryEncoder is implemented by types that can encode themselves into the
// PostgreSQL binary wire format.
type BinaryEncoder interface {
	// EncodeBinary should encode the binary format of self to w. If self is the
	// SQL value NULL then write nothing and return (true, nil). The caller of
	// EncodeBinary is responsible for writing the correct NULL value or the
	// length of the data written.
	EncodeBinary(w io.Writer) (null bool, err error)
}

// TextEncoder is implemented by types that can encode themselves into the
// PostgreSQL text wire format.
type TextEncoder interface {
	// EncodeText should encode the text format of self to w. If self is the SQL
	// value NULL then write nothing and return (true, nil). The caller of
	// EncodeText is responsible for writing the correct NULL value or the length
	// of the data written.
	EncodeText(w io.Writer) (null bool, err error)
}

var errUndefined = errors.New("cannot encode status undefined")

type DataType struct {
	Name string
	Type reflect.Type
	Oid  Oid
}

type ConnInfo struct {
	oidToDataType         map[Oid]*DataType
	nameToDataType        map[string]*DataType
	reflectTypeToDataType map[reflect.Type]*DataType
}

func NewConnInfo() *ConnInfo {
	return &ConnInfo{
		oidToDataType:         make(map[Oid]*DataType, 256),
		nameToDataType:        make(map[string]*DataType, 256),
		reflectTypeToDataType: make(map[reflect.Type]*DataType, 256),
	}
}

func NewDefaultConnInfo() *ConnInfo {
	ci := NewConnInfo()

	// TODO - probably better if exact 1 to 1 mapping between oids and types. So might need to separate text and varchar and inet and cidr
	dataTypes := []DataType{
		{Name: "_aclitem", Type: reflect.TypeOf(AclitemArray{}), Oid: AclitemArrayOid},
		{Name: "_bool", Type: reflect.TypeOf(BoolArray{}), Oid: BoolArrayOid},
		{Name: "_bytea", Type: reflect.TypeOf(ByteaArray{}), Oid: ByteaArrayOid},
		{Name: "_cidr", Type: reflect.TypeOf(CidrArray{}), Oid: CidrArrayOid},
		{Name: "aclitem", Type: reflect.TypeOf(Aclitem{}), Oid: AclitemOid},
		{Name: "bool", Type: reflect.TypeOf(Bool{}), Oid: BoolOid},
		{Name: "bytea", Type: reflect.TypeOf(Bytea{}), Oid: ByteaOid},
		{Name: "char", Type: reflect.TypeOf(QChar{}), Oid: CharOid},
		{Name: "cid", Type: reflect.TypeOf(Cid{}), Oid: CidOid},
		{Name: "cidr", Type: reflect.TypeOf(CidrArray{}), Oid: CidrOid},
		{Name: "_date", Type: reflect.TypeOf(DateArray{}), Oid: DateArrayOid},
		{Name: "date", Type: reflect.TypeOf(Date{}), Oid: DateOid},
		{Name: "_float4", Type: reflect.TypeOf(Float4Array{}), Oid: Float4ArrayOid},
		{Name: "float4", Type: reflect.TypeOf(Float4{}), Oid: Float4Oid},
		{Name: "_float8", Type: reflect.TypeOf(Float8Array{}), Oid: Float8ArrayOid},
		{Name: "float8", Type: reflect.TypeOf(Float8{}), Oid: Float8Oid},
		{Name: "_inet", Type: reflect.TypeOf(InetArray{}), Oid: InetArrayOid},
		{Name: "inet", Type: reflect.TypeOf(Inet{}), Oid: InetOid},
		{Name: "_int2", Type: reflect.TypeOf(Int2Array{}), Oid: Int2ArrayOid},
		{Name: "int2", Type: reflect.TypeOf(Int2{}), Oid: Int2Oid},
		{Name: "_int4", Type: reflect.TypeOf(Int4Array{}), Oid: Int4ArrayOid},
		{Name: "int4", Type: reflect.TypeOf(Int4{}), Oid: Int4Oid},
		{Name: "_int8", Type: reflect.TypeOf(Int8Array{}), Oid: Int8ArrayOid},
		{Name: "int8", Type: reflect.TypeOf(Int8{}), Oid: Int8Oid},
		{Name: "jsonb", Type: reflect.TypeOf(Jsonb{}), Oid: JsonbOid},
		{Name: "json", Type: reflect.TypeOf(Json{}), Oid: JsonOid},
		{Name: "name", Type: reflect.TypeOf(Name{}), Oid: NameOid},
		{Name: "oid", Type: reflect.TypeOf(OidValue{}), Oid: OidOid},
		{Name: "_text", Type: reflect.TypeOf(TextArray{}), Oid: TextArrayOid},
		{Name: "text", Type: reflect.TypeOf(Text{}), Oid: TextOid},
		{Name: "tid", Type: reflect.TypeOf(Tid{}), Oid: TidOid},
		{Name: "_timestamp", Type: reflect.TypeOf(TimestampArray{}), Oid: TimestampArrayOid},
		{Name: "timestamp", Type: reflect.TypeOf(Timestamp{}), Oid: TimestampOid},
		{Name: "_timestamptz", Type: reflect.TypeOf(TimestamptzArray{}), Oid: TimestamptzArrayOid},
		{Name: "timestamptz", Type: reflect.TypeOf(Timestamptz{}), Oid: TimestamptzOid},
		{Name: "_varchar", Type: reflect.TypeOf(VarcharArray{}), Oid: VarcharArrayOid},
		{Name: "varchar", Type: reflect.TypeOf(Text{}), Oid: VarcharOid},
		{Name: "xid", Type: reflect.TypeOf(Xid{}), Oid: XidOid},
	}

	for _, dt := range dataTypes {
		ci.RegisterDataType(dt)
	}

}

func (ci *ConnInfo) RegisterDataType(t DataType) {
	ci.oidToDataType[t.Oid] = &t
	ci.nameToDataType[t.Name] = &t
	ci.reflectTypeToDataType[t.Type] = &t
}

func (ci *ConnInfo) DataTypeForOid(oid Oid) *DataType {
	return ci.oidToDataType[oid]
}

func (ci *ConnInfo) DataTypeForName(name string) *DataType {
	return ci.nameToDataType[name]
}

func (ci *ConnInfo) DataTypeForReflectType(t reflect.Type) *DataType {
	return ci.reflectTypeToDataType[t]
}
