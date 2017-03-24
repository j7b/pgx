package pgx

import (
	"fmt"
	"io"
	"strings"
)

type CopierOptions interface {
	o() int
	string() string
}

type copierformat string

func (copierformat) o() int { return 1 }

func (c copierformat) string() string {
	return fmt.Sprintf(`FORMAT '%s'`, c)
}

const (
	CSVFormat    copierformat = "csv"
	TextFormat   copierformat = "text"
	BinaryFormat copierformat = "binary"
)

type CopierOIDs bool

func (CopierOIDs) o() int { return 2 }

func (c CopierOIDs) string() string {
	return fmt.Sprintf(`OIDS %v`, c)
}

type CopierDelimiter rune

func (CopierDelimiter) o() int { return 3 }

func (c CopierDelimiter) string() string {
	return fmt.Sprintf(`DELIMITER '%s'`, string(c))
}

type CopierNullString string

func (CopierNullString) o() int { return 4 }

func (c CopierNullString) string() string {
	return fmt.Sprintf(`NULL '%s'`, c)
}

type CopierHeader bool

func (CopierHeader) o() int { return 5 }

func (c CopierHeader) string() string {
	return fmt.Sprintf(`HEADER %v`, c)
}

type CopierQuote rune

func (CopierQuote) o() int { return 6 }

func (c CopierQuote) string() string {
	s := string(c)
	if s == `'` {
		s = `''`
	}
	return fmt.Sprintf(`QUOTE '%s'`, s)
}

type CopierEscape rune

func (CopierEscape) o() int { return 7 }

func (c CopierEscape) string() string {
	return fmt.Sprintf(`ESCAPE '%s'`, string(c))
}

type CopierForceQuoteColumns []string

func (CopierForceQuoteColumns) o() int { return 8 }

func (c CopierForceQuoteColumns) string() string {
	if len(c) == 0 {
		return ""
	}
	if len(c) == 1 && c[0] == `*` {
		return fmt.Sprintf(`FORCE_QUOTE *`)
	}
	return fmt.Sprintf(`FORCE_QUOTE (%s)`, strings.Join(c, `,`))
}

type CopierForceNotNullColumns []string

func (CopierForceNotNullColumns) o() int { return 9 }

func (c CopierForceNotNullColumns) string() string {
	if len(c) == 0 {
		return ""
	}
	return fmt.Sprintf(`FORCE_NOT_NULL (%s)`, strings.Join(c, `,`))
}

type CopierEncoding string

func (CopierEncoding) o() int { return 10 }

func (s CopierEncoding) string() string {
	return fmt.Sprintf(`ENCODING '%s'`, s)
}

type Copier struct {
	opts map[int]CopierOptions
}

func NewCopier(options ...CopierOptions) (*Copier, error) {
	c := new(Copier)
	for _, o := range options {
		if c.opts == nil {
			c.opts = make(map[int]CopierOptions)
		}
		if _, found := c.opts[o.o()]; found {
			return nil, fmt.Errorf("copier: option type %T specified twice", o)
		}
		c.opts[o.o()] = o
	}
	return c, nil
}

func (co *Copier) options() string {
	if len(co.opts) == 0 {
		return ""
	}
	options := make([]string, 0, len(co.opts))
	for _, v := range co.opts {
		s := v.string()
		if len(s) > 0 {
			options = append(options, s)
		}
	}
	return fmt.Sprintf(` WITH (%s)`, strings.Join(options, `,`))
}

type CopyType interface {
	Table(regclass Identifier, columns ...string) FromTo
	Query(q string, args ...interface{}) To
	ct()
}

type copytype struct {
	tx      *Tx
	options *Copier
}

func (ct *copytype) Table(name Identifier, columns ...string) FromTo {
	return &copyprep{
		tx:      ct.tx,
		options: ct.options,
		table:   name,
		columns: columns,
	}
}

func (ct *copytype) Query(q string, args ...interface{}) To {
	return &copyprep{
		tx:      ct.tx,
		options: ct.options,
		query:   q,
		args:    args,
	}
}

func (*copytype) ct() {}

type To interface {
	To(io.Writer) error
	cp()
}

type FromTo interface {
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

func (cp *copyprep) copyout(conn *Conn, w io.Writer) error {
	_, err := getmessage(conn, int('H'))
	if err != nil {
		return err
	}
	var copydone bool
	for !copydone {
		t, r, err := conn.rxMsg()
		if err != nil {
			conn.die(err)
			return err
		}
		switch t {
		case copyDone:
			copydone = true
		case copyData:
			_, err = w.Write(r.readBytes(r.msgBytesRemaining))
			if err != nil {
				conn.die(err)
				return err
			}
		case errorResponse:
			err = conn.rxErrorResponse(r)
			conn.die(err)
			return err
		default:
			err = conn.processContextFreeMsg(t, r)
			if err != nil {
				conn.die(err)
				return err
			}
		}
	}
	_, err = getmessage(conn, commandComplete)
	if err != nil {
		return err
	}
	_, err = getmessage(conn, readyForQuery)
	if err != nil {
		return err
	}
	return nil
}

func (cp *copyprep) copyin(conn *Conn, r io.Reader) error {
	_, err := getmessage(conn, copyInResponse)
	if err != nil {
		return err
	}
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
	_, err = getmessage(conn, commandComplete)
	if err != nil {
		return err
	}
	_, err = getmessage(conn, readyForQuery)
	if err != nil {
		return err
	}
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
	return cp.copyin(conn, r)
}

func (cp *copyprep) totable(conn *Conn, w io.Writer) error {
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
	err := conn.sendSimpleQuery(fmt.Sprintf("copy %s%s to stdout %s;", table, cols, options))
	if err != nil {
		return err
	}
	return cp.copyout(conn, w)
}

func (cp *copyprep) toquery(conn *Conn, w io.Writer) error {
	return fmt.Errorf("TODO")
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
	return fmt.Errorf("copy: query not possible here")
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
	if istable {
		return cp.totable(conn, w)
	}
	return cp.toquery(conn, w)
}

func (*copyprep) cp() {}

func (co *Copier) Copy(tx *Tx) CopyType {
	return &copytype{options: co, tx: tx}
}

func getmessage(conn *Conn, want int) (*msgReader, error) {
	for {
		t, r, err := conn.rxMsg()
		if err != nil {
			return nil, err
		}
		switch t {
		case byte(want):
			return r, nil
		case errorResponse:
			return nil, conn.rxErrorResponse(r)
		default:
			err = conn.processContextFreeMsg(t, r)
			if err != nil {
				return nil, err
			}
		}
	}
}
