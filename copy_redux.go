package pgx

import (
	"fmt"
	"io"
	"strings"
)

type CopierOptions interface {
	o()
}

type copierformat string

func (copierformat) o() {}

const (
	CSVFormat    copierformat = "csv"
	TextFormat   copierformat = "text"
	BinaryFormat copierformat = "binary"
)

type CopierOIDs bool

func (CopierOIDs) o() {}

type CopierDelimiter rune

func (CopierDelimiter) o() {}

type CopierNullString string

func (CopierNullString) o() {}

type CopierHeader bool

func (CopierHeader) o() {}

type CopierQuote rune

func (CopierQuote) o() {}

type CopierEscape rune

func (CopierEscape) o() {}

type CopierForceQuoteColumns []string

func (CopierForceQuoteColumns) o() {}

type CopierForceNotNullColumns []string

func (CopierForceNotNullColumns) o() {}

type CopierEncoding string

func (CopierEncoding) o() {}

type Copier struct {
	format       string   // FORMAT format_name
	oids         bool     // OIDS [ boolean ]
	delimiter    rune     // DELIMITER 'delimiter_character'
	null         string   // NULL 'null_string'
	header       bool     // HEADER [ boolean ]
	quote        rune     // QUOTE 'quote_character'
	escape       rune     // ESCAPE 'escape_character'
	forcequote   []string // FORCE_QUOTE { ( column_name [, ...] ) | * }
	forcenotnull []string // FORCE_NOT_NULL ( column_name [, ...] )
	encoding     string   // ENCODING 'encoding_name'
}

func NewCopier(options ...CopierOptions) *Copier {
	c := new(Copier)
	for _, o := range options {
		switch t := o.(type) {
		case copierformat:
			c.format = string(t)
		case CopierOIDs:
			c.oids = bool(t)
		case CopierDelimiter:
			c.delimiter = rune(t)
		case CopierNullString:
			c.null = string(t)
		case CopierHeader:
			c.header = bool(t)
		case CopierQuote:
			c.quote = rune(t)
		case CopierEscape:
			c.escape = rune(t)
		case CopierForceQuoteColumns:
			c.forcequote = []string(t)
		case CopierForceNotNullColumns:
			c.forcenotnull = []string(t)
		case CopierEncoding:
			c.encoding = string(t)
		}
	}
	return c
}

func (co *Copier) options() string {
	options := make([]string, 0, 1)
	if co.format != "" {
		options = append(options, fmt.Sprintf(`FORMAT '%s'`, co.format))
	}
	if len(options) > 0 {
		return fmt.Sprintf(`(%s)`, strings.Join(options, `,`))
	}
	return ""
}

type CopyType interface {
	Table(regclass Identifier, columns ...string) CopyPrep
	Query(q string, args ...interface{}) CopyPrep
	ct()
}

type copytype struct {
	tx      *Tx
	options *Copier
}

func (ct *copytype) Table(name Identifier, columns ...string) CopyPrep {
	return &copyprep{
		tx:      ct.tx,
		options: ct.options,
		table:   name,
		columns: columns,
	}
}

func (ct *copytype) Query(q string, args ...interface{}) CopyPrep {
	return &copyprep{
		tx:      ct.tx,
		options: ct.options,
		query:   q,
		args:    args,
	}
}

func (*copytype) ct() {}

type CopyPrep interface {
	From(io.Reader) error
	To(io.Writer) error
	cp()
}

type copyprep struct {
	tx      *Tx
	options *Copier
	table   Identifier
	columns []string
	query   string
	args    []interface{}
}

func (cp *copyprep) conn() (*Conn, error) {
	if cp.tx == nil {
		return nil, fmt.Errorf("copy: nil transaction")
	}
	if cp.tx.Status() != 0 {
		return nil, fmt.Errorf("copy: bad tx status %v", cp.tx.Status())
	}
	return cp.tx.Conn(), nil
}

func (cp *copyprep) checktq() (table bool, err error) {
	switch {
	case len(cp.table) > 0 && len(cp.query) > 0:
		return false, fmt.Errorf("copy: both table and query prepped")
	case len(cp.table) > 0:
		return true, nil
	case len(cp.query) > 0:
		return false, nil
	}
	err = fmt.Errorf("copy: no table or query specified")
	return
}

func (cp *copyprep) fromquery(r io.Reader) error {
	return nil
}

func (cp *copyprep) fromtable(conn *Conn, r io.Reader) error {
	var columns []string
	if len(cp.columns) > 0 {
		columns = make([]string, len(cp.columns))
		for i := range cp.columns {
			columns[i] = quoteIdentifier(cp.columns[i])
		}
	}
	var cols string
	if len(columns) > 0 {
		cols = fmt.Sprintf(`(%s)`, strings.Join(columns, `,`))
	}
	table := cp.table.Sanitize()
	options := cp.options.options()
	err := conn.sendSimpleQuery(fmt.Sprintf("copy %s%s from stdin %s;", table, cols, options))
	if err != nil {
		return err
	}
	if err = conn.readUntilCopyInResponse(); err != nil {
		return err
	}
	/*
		wb := newWriteBuf(conn, copyFail)
		isuck := []byte("I suck")
		wb.WriteCString(string(isuck))
		wb.closeMsg()
		println(string(wb.buf))
		_, err = conn.conn.Write(wb.buf)
		if err != nil {
			conn.die(err)
			return err
		}
		for {
			t, r, err := conn.rxMsg()
			if err != nil {
				return err
			}
			switch t {
			case commandComplete:
				return nil
			case errorResponse:
				return conn.rxErrorResponse(r)
			default:
				err = conn.processContextFreeMsg(t, r)
				if err != nil {
					return err
				}
			}
		}
	*/

	var n int
	buf := make([]byte, 65536)
	wb := newWriteBuf(conn, copyData)
	for err == nil {
		n, err = r.Read(buf)
		if n > 0 {
			wb.WriteBytes(buf[:n])
			wb.closeMsg()
			_, err = conn.conn.Write(wb.buf)
			if err != nil {
				conn.die(err)
				return err
			}
			wb = newWriteBuf(conn, copyData)
		}
	}
	if err != io.EOF {
		wb = newWriteBuf(conn, copyFail)
		wb.WriteCString(err.Error())
		wb.closeMsg()
		if _, err := conn.conn.Write(wb.buf); err != nil {
			conn.die(err)
			return err
		}
		return err
	}
	wb = newWriteBuf(conn, copyDone)
	wb.closeMsg()
	_, err = conn.conn.Write(wb.buf)
	if err != nil {
		conn.die(err)
		return err
	}
	var readyforquery bool
	for !readyforquery {
		t, r, err := conn.rxMsg()
		if err != nil {
			return err
		}
		switch t {
		case commandComplete:
			t, r, err = conn.rxMsg()
			if err != nil {
				return err
			}
			if t != readyForQuery {
				return fmt.Errorf("not ready for query")
			}
			readyforquery = true
		case errorResponse:
			return conn.rxErrorResponse(r)
		default:
			err = conn.processContextFreeMsg(t, r)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (cp *copyprep) From(r io.Reader) error {
	conn, err := cp.conn()
	if err != nil {
		return err
	}
	istable, err := cp.checktq()
	if err != nil {
		return err
	}
	if istable {
		return cp.fromtable(conn, r)
	}
	return cp.fromquery(r)
}

func (cp *copyprep) To(w io.Writer) error {
	conn, err := cp.conn()
	if err != nil {
		return err
	}
	istable, err := cp.checktq()
	if err != nil {
		return err
	}
	_, _ = conn, istable
	return nil
}

func (*copyprep) cp() {}

func (co *Copier) Copy(tx *Tx) CopyType {
	return &copytype{options: co, tx: tx}
}
