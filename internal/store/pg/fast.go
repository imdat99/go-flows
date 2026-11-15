package pg

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The fast path runs the hot-path batches (workflow task completion,
// activity completion with resume) on pgconn directly, below pgx's
// Batch/Query layer, after saga's fast path (saga/internal/store/fast.go).
//
// Why: pgx boxes every argument into an interface, builds a pipeline queue
// (container/list) per batch, plans encoding per argument and scanning per
// column: about 320 allocations per activity in the eager loop. Here:
//   - statements are prepared once per connection under a fixed name;
//   - parameters are encoded in binary, by the parameter types the server
//     reported when preparing, into one pooled buffer (arrays included, in
//     Postgres' binary array format);
//   - results come back in binary and are decoded by hand;
//   - a pgconn.Batch (one Sync: one implicit transaction, one round trip)
//     is reused from a pool.
//
// The fast path needs prepared statements, so it is used only when the pool
// runs in QueryExecModeCacheStatement (the default; other modes are how
// PgBouncer in transaction mode is usually configured) and has no query
// tracer (it would not see these statements). Otherwise the same SQL goes
// through pgx.

// fastPathEnabled reports whether pool's configuration allows the fast path.
func fastPathEnabled(pool *pgxpool.Pool) bool {
	cc := pool.Config().ConnConfig
	return cc.DefaultQueryExecMode == pgx.QueryExecModeCacheStatement && cc.Tracer == nil
}

// stmtName names a statement after its text, so that a name always maps to
// one text on every connection.
func stmtName(sql string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(sql))
	return "flows_" + strconv.FormatUint(h.Sum64(), 36)
}

// Postgres type OIDs the fast path encodes.
const (
	oidBool        = 16
	oidBytea       = 17
	oidInt8        = 20
	oidInt2        = 21
	oidInt4        = 23
	oidText        = 25
	oidVarchar     = 1043
	oidTimestamptz = 1184
	oidBoolArr     = 1000
	oidByteaArr    = 1001
	oidInt2Arr     = 1005
	oidInt4Arr     = 1007
	oidTextArr     = 1009
	oidInt8Arr     = 1016
	oidTstzArr     = 1185
)

// errParamType reports a statement whose parameter types do not match what
// the fast path encodes: a bug in this package (the SQL needs a cast).
var errParamType = errors.New("pg fast path: unexpected parameter type")

// pgEpochUnix is 2000-01-01 00:00:00 UTC, the epoch of binary timestamps.
const pgEpochUnix = 946684800

// tstzMicros converts to microseconds since the Postgres epoch, without the
// 292-year saturation of time.Duration arithmetic.
func tstzMicros(t time.Time) int64 {
	return (t.Unix()-pgEpochUnix)*1_000_000 + int64(t.Nanosecond()/1000)
}

// args encodes the parameters of one statement in binary into a shared
// buffer. Values are recorded as offsets and materialized by finish, since
// the buffer may grow while encoding.
type args struct {
	sd   *pgconn.StatementDescription // current statement
	oids []uint32                     // its parameter types
	n    int                          // parameters encoded so far for the current statement
	err  error
}

// fastBatch builds a pgconn.Batch of prepared statements with binary
// parameters. Reused through fastBatchPool.
type fastBatch struct {
	conn  *pgx.Conn
	ctx   context.Context
	batch pgconn.Batch
	buf   []byte     // parameter bytes of every statement
	spans [][2]int32 // offsets of each value in buf, -1 start for NULL
	vals  [][]byte
	fmts  []int16 // all 1 (binary)
	res   []int16 // result formats: all binary
	a     args
	stmts int
}

var fastBatchPool = sync.Pool{New: func() any {
	return &fastBatch{res: []int16{1}}
}}

const maxPooledBuf = 64 << 10

func getFastBatch(ctx context.Context, conn *pgx.Conn) *fastBatch {
	fb, ok := fastBatchPool.Get().(*fastBatch)
	if !ok { // the pool only holds *fastBatch
		fb = &fastBatch{res: []int16{1}}
	}
	fb.conn, fb.ctx = conn, ctx
	return fb
}

func (fb *fastBatch) release() {
	fb.conn, fb.ctx = nil, nil
	fb.batch = pgconn.Batch{}
	if cap(fb.buf) > maxPooledBuf {
		return // do not pin a large buffer
	}
	fb.buf, fb.spans, fb.a, fb.stmts = fb.buf[:0], fb.spans[:0], args{}, 0
	clear(fb.vals)
	fb.vals = fb.vals[:0]
	fastBatchPool.Put(fb)
}

// begin starts statement sql: it is prepared on the connection (once; pgx
// caches the description by name).
func (fb *fastBatch) begin(sql string, name string) {
	if fb.a.err != nil {
		return
	}
	sd, err := fb.conn.Prepare(fb.ctx, name, sql)
	if err != nil {
		fb.a.err = err
		return
	}
	fb.a.sd, fb.a.oids, fb.a.n = sd, sd.ParamOIDs, 0
	fb.spans = fb.spans[:0]
}

// end appends the statement with its encoded parameters to the batch.
func (fb *fastBatch) end(name string) {
	if fb.a.err != nil {
		return
	}
	if fb.a.n != len(fb.a.oids) {
		fb.a.err = fmt.Errorf("%w: %s: %d parameters encoded, %d expected", errParamType, name, fb.a.n, len(fb.a.oids))
		return
	}
	// Values are views into buf; the batch copies them into its own buffer
	// when encoding the Bind message, so buf can be reused afterwards.
	fb.vals = fb.vals[:0]
	for _, sp := range fb.spans {
		if sp[0] < 0 {
			fb.vals = append(fb.vals, nil)
			continue
		}
		fb.vals = append(fb.vals, fb.buf[sp[0]:sp[1]])
	}
	for len(fb.fmts) < len(fb.vals) {
		fb.fmts = append(fb.fmts, 1)
	}
	// With the cached description, no Describe Portal round of messages
	// (and no row description to decode) for statements returning rows.
	fb.batch.ExecStatement(fb.a.sd, fb.vals, fb.fmts[:len(fb.vals)], fb.res)
	fb.buf = fb.buf[:0]
	fb.stmts++
}

// next returns the type of the next parameter, or 0 after an error.
func (fb *fastBatch) next() uint32 {
	if fb.a.err != nil {
		return 0
	}
	if fb.a.n >= len(fb.a.oids) {
		fb.a.err = fmt.Errorf("%w: too many parameters", errParamType)
		return 0
	}
	oid := fb.a.oids[fb.a.n]
	fb.a.n++
	return oid
}

func (fb *fastBatch) bad(oid uint32, what string) {
	if fb.a.err == nil {
		fb.a.err = fmt.Errorf("%w: parameter %d is OID %d, got %s", errParamType, fb.a.n, oid, what)
	}
}

func (fb *fastBatch) span(start int) {
	fb.spans = append(fb.spans, [2]int32{int32(start), int32(len(fb.buf))})
} // batch buffers stay far below 2 GiB

func (fb *fastBatch) null() { fb.spans = append(fb.spans, [2]int32{-1, -1}) }

func appendInt(buf []byte, oid uint32, v int64) ([]byte, bool) {
	switch oid {
	case oidInt8:
		return binary.BigEndian.AppendUint64(buf, uint64(v)), true // two's complement on the wire
	case oidInt4:
		return binary.BigEndian.AppendUint32(buf, uint32(v)), true // values are int32 by construction
	case oidInt2:
		return binary.BigEndian.AppendUint16(buf, uint16(v)), true // values are int16 by construction
	}
	return buf, false
}

// Int encodes an integer parameter (int2, int4 or int8 per the statement).
func (fb *fastBatch) Int(v int64) {
	oid := fb.next()
	start := len(fb.buf)
	var ok bool
	if fb.buf, ok = appendInt(fb.buf, oid, v); !ok {
		fb.bad(oid, "an integer")
		return
	}
	fb.span(start)
}

// Str encodes a text parameter.
func (fb *fastBatch) Str(v string) {
	if oid := fb.next(); oid != oidText && oid != oidVarchar {
		fb.bad(oid, "text")
		return
	}
	start := len(fb.buf)
	fb.buf = append(fb.buf, v...)
	fb.span(start)
}

// Bytes encodes a bytea parameter; nil is NULL.
func (fb *fastBatch) Bytes(v []byte) {
	if oid := fb.next(); oid != oidBytea {
		fb.bad(oid, "bytea")
		return
	}
	if v == nil {
		fb.null()
		return
	}
	start := len(fb.buf)
	fb.buf = append(fb.buf, v...)
	fb.span(start)
}

// Bool encodes a boolean parameter.
func (fb *fastBatch) Bool(v bool) {
	if oid := fb.next(); oid != oidBool {
		fb.bad(oid, "bool")
		return
	}
	start := len(fb.buf)
	b := byte(0)
	if v {
		b = 1
	}
	fb.buf = append(fb.buf, b)
	fb.span(start)
}

// arrayHeader writes a one-dimensional array header for n elements of elem.
func (fb *fastBatch) arrayHeader(n int, elem uint32, hasNull bool) {
	fb.buf = binary.BigEndian.AppendUint32(fb.buf, 1) // ndim
	nulls := uint32(0)
	if hasNull {
		nulls = 1
	}
	fb.buf = binary.BigEndian.AppendUint32(fb.buf, nulls)
	fb.buf = binary.BigEndian.AppendUint32(fb.buf, elem)
	fb.buf = binary.BigEndian.AppendUint32(fb.buf, uint32(n)) // small arrays
	fb.buf = binary.BigEndian.AppendUint32(fb.buf, 1)         // lower bound
}

func elemOID(arr uint32) (uint32, uint32) { // element type and its size (0: variable)
	switch arr {
	case oidInt8Arr:
		return oidInt8, 8
	case oidInt4Arr:
		return oidInt4, 4
	case oidInt2Arr:
		return oidInt2, 2
	case oidBoolArr:
		return oidBool, 1
	case oidTstzArr:
		return oidTimestamptz, 8
	case oidTextArr:
		return oidText, 0
	case oidByteaArr:
		return oidBytea, 0
	}
	return 0, 0
}

// Ints encodes an integer array parameter from n values got through at.
func (fb *fastBatch) Ints(n int, at func(i int) int64) {
	oid := fb.next()
	elem, size := elemOID(oid)
	if size == 0 || (elem != oidInt8 && elem != oidInt4 && elem != oidInt2) {
		fb.bad(oid, "an integer array")
		return
	}
	start := len(fb.buf)
	fb.arrayHeader(n, elem, false)
	for i := range n {
		fb.buf = binary.BigEndian.AppendUint32(fb.buf, size)
		fb.buf, _ = appendInt(fb.buf, elem, at(i))
	}
	fb.span(start)
}

// Bools encodes a boolean array parameter.
func (fb *fastBatch) Bools(n int, at func(i int) bool) {
	if oid := fb.next(); oid != oidBoolArr {
		fb.bad(oid, "a bool array")
		return
	}
	start := len(fb.buf)
	fb.arrayHeader(n, oidBool, false)
	for i := range n {
		fb.buf = binary.BigEndian.AppendUint32(fb.buf, 1)
		b := byte(0)
		if at(i) {
			b = 1
		}
		fb.buf = append(fb.buf, b)
	}
	fb.span(start)
}

// Strs encodes a text array parameter.
func (fb *fastBatch) Strs(n int, at func(i int) string) {
	if oid := fb.next(); oid != oidTextArr {
		fb.bad(oid, "a text array")
		return
	}
	start := len(fb.buf)
	fb.arrayHeader(n, oidText, false)
	for i := range n {
		s := at(i)
		fb.buf = binary.BigEndian.AppendUint32(fb.buf, uint32(len(s))) // short strings
		fb.buf = append(fb.buf, s...)
	}
	fb.span(start)
}

// Blobs encodes a bytea array parameter; nil elements are NULL.
func (fb *fastBatch) Blobs(n int, at func(i int) []byte) {
	if oid := fb.next(); oid != oidByteaArr {
		fb.bad(oid, "a bytea array")
		return
	}
	hasNull := false
	for i := range n {
		if at(i) == nil {
			hasNull = true
		}
	}
	start := len(fb.buf)
	fb.arrayHeader(n, oidBytea, hasNull)
	for i := range n {
		b := at(i)
		if b == nil {
			fb.buf = binary.BigEndian.AppendUint32(fb.buf, 0xFFFFFFFF)
			continue
		}
		fb.buf = binary.BigEndian.AppendUint32(fb.buf, uint32(len(b))) // payloads stay far below 4 GiB
		fb.buf = append(fb.buf, b...)
	}
	fb.span(start)
}

// Times encodes a timestamptz array parameter; zero times are NULL.
func (fb *fastBatch) Times(n int, at func(i int) time.Time) {
	if oid := fb.next(); oid != oidTstzArr {
		fb.bad(oid, "a timestamptz array")
		return
	}
	hasNull := false
	for i := range n {
		if at(i).IsZero() {
			hasNull = true
		}
	}
	start := len(fb.buf)
	fb.arrayHeader(n, oidTimestamptz, hasNull)
	for i := range n {
		t := at(i)
		if t.IsZero() {
			fb.buf = binary.BigEndian.AppendUint32(fb.buf, 0xFFFFFFFF)
			continue
		}
		fb.buf = binary.BigEndian.AppendUint32(fb.buf, 8)
		fb.buf = binary.BigEndian.AppendUint64(fb.buf, uint64(tstzMicros(t))) // two's complement on the wire
	}
	fb.span(start)
}

// exec sends the batch and calls read for each statement's result, in
// order; read gets the result reader of statement i.
func (fb *fastBatch) exec(read func(i int, rr *pgconn.ResultReader) error) error {
	if fb.a.err != nil {
		return fb.a.err
	}
	mrr := fb.conn.PgConn().ExecBatch(fb.ctx, &fb.batch)
	var readErr error
	for i := 0; mrr.NextResult(); i++ {
		rr := mrr.ResultReader()
		if readErr == nil && read != nil {
			readErr = read(i, rr)
		}
		if _, err := rr.Close(); err != nil && readErr == nil {
			readErr = err
		}
	}
	if err := mrr.Close(); err != nil {
		if staleStatement(err) {
			_ = fb.conn.Close(fb.ctx) // the next connection prepares afresh
		}
		return err
	}
	return readErr
}

// staleStatement reports errors after which a prepared statement must not be
// reused (ported from saga): "cached plan must not change result type"
// (0A000) and "prepared statement does not exist" (26000).
func staleStatement(err error) bool {
	c := sqlState(err)
	return c == "0A000" || c == "26000"
}

var errBadValue = errors.New("pg fast path: unexpected binary value")

// decodeInt decodes a binary int2, int4 or int8.
func decodeInt(b []byte) (int64, error) {
	switch len(b) {
	case 2:
		return int64(int16(binary.BigEndian.Uint16(b))), nil // two's complement
	case 4:
		return int64(int32(binary.BigEndian.Uint32(b))), nil // two's complement
	case 8:
		return int64(binary.BigEndian.Uint64(b)), nil // two's complement
	}
	return 0, fmt.Errorf("%w: integer of %d bytes", errBadValue, len(b))
}

// decodeTime decodes a binary timestamptz; NULL is the zero time.
func decodeTime(b []byte) (time.Time, error) {
	if b == nil {
		return time.Time{}, nil
	}
	if len(b) != 8 {
		return time.Time{}, fmt.Errorf("%w: timestamptz of %d bytes", errBadValue, len(b))
	}
	us := int64(binary.BigEndian.Uint64(b)) // two's complement
	sec, rem := us/1_000_000, us%1_000_000
	if rem < 0 {
		sec, rem = sec-1, rem+1_000_000
	}
	return time.Unix(pgEpochUnix+sec, rem*1000).UTC(), nil
}

// cloneBytes copies a value out of the reader's buffer; NULL stays nil and
// an empty value stays non-nil.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte{}, b...)
}
