//go:build cgo

package sqlite

/*
#include <stdlib.h>
#include <sqlite3.h>

// These wrappers are necessary because SQLITE_TRANSIENT
// is a pointer constant, and cgo doesn't translate them correctly.
// The definition in sqlite3.h is:
//
// typedef void (*sqlite3_destructor_type)(void*);
// #define SQLITE_STATIC      ((sqlite3_destructor_type)0)
// #define SQLITE_TRANSIENT   ((sqlite3_destructor_type)-1)

static int my_bind_text(sqlite3_stmt *stmt, int n, char *p, int np) {
	return sqlite3_bind_text(stmt, n, p, np, SQLITE_TRANSIENT);
}
static int my_bind_blob(sqlite3_stmt *stmt, int n, void *p, int np) {
	return sqlite3_bind_blob(stmt, n, p, np, SQLITE_TRANSIENT);
}
*/
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"time"
	"unsafe"
)

type JournalMode string

const (
	JournalModeWAL = "wal"
)

func (j JournalMode) String() string {
	return string(j)
}

type logger interface {
	Println(v ...any)
}

type discardLogger struct{}

func (d *discardLogger) Println(v ...any) {}

type Options struct {
	BusyTimeout *time.Duration
	ForeignKeys *bool
	JournalMode JournalMode
	Logger      logger
	Name        string
}

func RegisterDriver(opts Options) {
	if opts.Name == "" {
		opts.Name = "sqlite"
	}

	if opts.Logger == nil {
		opts.Logger = &discardLogger{}
	}

	if opts.JournalMode == "" {
		opts.JournalMode = JournalModeWAL
	}

	if opts.BusyTimeout == nil {
		opts.BusyTimeout = ptr(5 * time.Second)
	}

	if opts.ForeignKeys == nil {
		opts.ForeignKeys = ptr(true)
	}

	sql.Register(opts.Name, &d{opts: opts, log: opts.Logger})
}

func ptr[T any](v T) *T {
	return &v
}

// d satisfies driver.Driver.
type d struct {
	opts Options
	log  logger
}

// Open returns a new connection to the database.
// The name is a string in a driver-specific format.
//
// Open may return a cached connection (one previously
// closed), but doing so is unnecessary; the sql package
// maintains a pool of idle connections for efficient re-use.
//
// The returned connection is only used by one goroutine at a
// time.
func (d *d) Open(name string) (driver.Conn, error) {
	var cC *C.sqlite3

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	// The default threading mode is serialized, but we set it explicitly: https://www.sqlite.org/threadsafe.html
	const flags = C.SQLITE_OPEN_READWRITE | C.SQLITE_OPEN_CREATE | C.SQLITE_OPEN_FULLMUTEX
	if cCode := C.sqlite3_open_v2(cName, &cC, flags, nil); cCode != C.SQLITE_OK {
		if cC != nil {
			// TODO handle return value
			C.sqlite3_close_v2(cC)
		}
		return nil, wrapErrorCode("error opening connection", cCode)
	}

	c := &connection{cC: cC}

	pragmas := map[string]any{
		"journal_mode": d.opts.JournalMode,
		"busy_timeout": d.opts.BusyTimeout.Milliseconds(),
		"foreign_keys": *d.opts.ForeignKeys,
	}

	for k, v := range pragmas {
		d.log.Println("Setting pragma", k, "to", v)
		if err := c.exec("pragma %v = %v", k, v); err != nil {
			return nil, wrapError("error setting pragma %v", err, k)
		}
	}

	return c, nil
}

func wrapError(format string, err error, args ...any) error {
	args = append(args, err)
	return fmt.Errorf(format+": %w", args...)
}

func wrapErrorCode(format string, cCode C.int, args ...any) error {
	args = append(args, errString(cCode))
	return fmt.Errorf(format+": %w", args...)
}

func errString(cCode C.int) error {
	return errors.New(C.GoString(C.sqlite3_errstr(cCode)))
}

// connection is a connection to a database. It is not used concurrently
// by multiple goroutines.
//
// connection is assumed to be stateful.
// connection satisfies driver.Conn.
type connection struct {
	cC *C.sqlite3
}

// Prepare returns a prepared statement, bound to this connection.
// See https://www.sqlite.org/c3ref/prepare.html
func (c *connection) Prepare(query string) (driver.Stmt, error) {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	var cStatement *C.sqlite3_stmt

	if cCode := C.sqlite3_prepare_v2(c.cC, cQuery, C.int(len(query)+1), &cStatement, nil); cCode != C.SQLITE_OK {
		return nil, wrapErrorCode(`error preparing statement for query "%v"`, cCode, query)
	}

	return &statement{connection: c, query: query, cStatement: cStatement}, nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
//
// Drivers must ensure all network calls made by Close
// do not block indefinitely (e.g. apply a timeout).
func (c *connection) Close() error {
	if cCode := C.sqlite3_close_v2(c.cC); cCode != C.SQLITE_OK {
		return wrapErrorCode("error closing connection", cCode)
	}
	c.cC = nil
	return nil
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (c *connection) Begin() (driver.Tx, error) {
	//TODO implement me
	panic("implement Begin")
}

// exec a query and interpolate args directly. For internal use only.
// See https://www.sqlite.org/c3ref/exec.html
func (c *connection) exec(format string, args ...any) error {
	query := fmt.Sprintf(format, args...)
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	if cCode := C.sqlite3_exec(c.cC, cQuery, nil, nil, nil); cCode != C.SQLITE_OK {
		return wrapErrorCode(`error running query "%v"`, cCode, query)
	}

	return nil
}

// statement is a prepared statement. It is bound to a connection and not
// used by multiple goroutines concurrently.
// statement satisfies driver.Stmt.
type statement struct {
	connection  *connection
	query       string
	cStatement  *C.sqlite3_stmt
	columnNames []string
}

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
//
// Drivers must ensure all network calls made by Close
// do not block indefinitely (e.g. apply a timeout).
// See https://www.sqlite.org/c3ref/finalize.html
func (s *statement) Close() error {
	if cCode := C.sqlite3_finalize(s.cStatement); cCode != C.SQLITE_OK {
		return wrapErrorCode(`error closing statement for query "%v"`, cCode, s.query)
	}
	return nil
}

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (s *statement) NumInput() int {
	return int(C.sqlite3_bind_parameter_count(s.cStatement))
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (s *statement) Exec(args []driver.Value) (driver.Result, error) {
	if len(args) > 0 {
		if err := s.bindArgs(args); err != nil {
			return nil, wrapError(`error binding args while executing query "%v"`, err, s.query)
		}
	}

	if cCode := C.sqlite3_step(s.cStatement); cCode != C.SQLITE_DONE && cCode != C.SQLITE_ROW {
		return nil, wrapErrorCode(`error executing query "%v"`, cCode, s.query)
	}

	lastInsertID := int64(C.sqlite3_last_insert_rowid(s.connection.cC))
	rowsAffected := int64(C.sqlite3_changes(s.connection.cC))

	return &result{lastInsertID: lastInsertID, rowsAffected: rowsAffected}, nil
}

// Query executes a query that may return rows, such as a
// SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (s *statement) Query(args []driver.Value) (driver.Rows, error) {
	if len(args) > 0 {
		if err := s.bindArgs(args); err != nil {
			return nil, wrapError(`error binding args while executing query "%v"`, err, s.query)
		}
	}

	if s.columnNames == nil {
		columnCount := int64(C.sqlite3_column_count(s.cStatement))
		s.columnNames = make([]string, columnCount)
		for i := range s.columnNames {
			s.columnNames[i] = C.GoString(C.sqlite3_column_name(s.cStatement, C.int(i)))
		}
	}

	return &rows{statement: s}, nil
}

func (s *statement) bindArgs(args []driver.Value) error {
	for i, arg := range args {
		// Variable index starts at 1 in SQLite
		idx := C.int(i + 1)

		switch arg := arg.(type) {
		case nil:
			if cCode := C.sqlite3_bind_null(s.cStatement, idx); cCode != C.SQLITE_OK {
				return wrapErrorCode("error binding nil arg at position %v", cCode, i)
			}

		case bool:
			argAsInt := 0
			if arg {
				argAsInt = 1
			}
			if cCode := C.sqlite3_bind_int64(s.cStatement, idx, C.sqlite3_int64(argAsInt)); cCode != C.SQLITE_OK {
				return wrapErrorCode("error binding bool arg at position %v", cCode, i)
			}

		case int64:
			if cCode := C.sqlite3_bind_int64(s.cStatement, idx, C.sqlite3_int64(arg)); cCode != C.SQLITE_OK {
				return wrapErrorCode("error binding int64 arg at position %v", cCode, i)
			}

		case float64:
			if cCode := C.sqlite3_bind_double(s.cStatement, idx, C.double(arg)); cCode != C.SQLITE_OK {
				return wrapErrorCode("error binding float64 arg at position %v", cCode, i)
			}

		case []byte:
			var p *byte
			if len(arg) > 0 {
				p = &arg[0]
			}
			if cCode := C.my_bind_blob(s.cStatement, idx, unsafe.Pointer(p), C.int(len(arg))); cCode != C.SQLITE_OK {
				return wrapErrorCode("error binding []byte arg at position %v", cCode, i)
			}

		case string:
			cArg := C.CString(arg)
			cCode := C.my_bind_text(s.cStatement, idx, cArg, C.int(len(arg)))
			C.free(unsafe.Pointer(cArg))
			if cCode != C.SQLITE_OK {
				return wrapErrorCode("error binding string arg at position %v", cCode, i)
			}

		default:
			return fmt.Errorf("unsupported arg type %T", arg)
		}
	}

	return nil
}

// rows is an iterator over an executed query's results.
// rows satisfies driver.Rows.
type rows struct {
	statement *statement
	err       error
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *rows) Columns() []string {
	return r.statement.columnNames
}

// Close closes the rows iterator.
func (r *rows) Close() error {
	r.statement = nil
	return r.err
}

const maxSlice = 1<<31 - 1

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
// See https://www.sqlite.org/c3ref/step.html
func (r *rows) Next(dest []driver.Value) error {
	cCode := C.sqlite3_step(r.statement.cStatement)

	if cCode == C.SQLITE_DONE {
		return io.EOF
	}

	// If next row is not ready
	if cCode != C.SQLITE_ROW {
		return wrapErrorCode(`error getting next row for query "%v"`, cCode, r.statement.query)
	}

	for i := range dest {
		switch cT := C.sqlite3_column_type(r.statement.cStatement, C.int(i)); cT {
		case C.SQLITE_INTEGER:
			dest[i] = int64(C.sqlite3_column_int64(r.statement.cStatement, C.int(i)))

		case C.SQLITE_FLOAT:
			dest[i] = float64(C.sqlite3_column_double(r.statement.cStatement, C.int(i)))

		case C.SQLITE_BLOB, C.SQLITE_TEXT:
			var b []byte
			n := int(C.sqlite3_column_bytes(r.statement.cStatement, C.int(i)))
			if n > 0 {
				p := C.sqlite3_column_blob(r.statement.cStatement, C.int(i))
				b = (*[maxSlice]byte)(unsafe.Pointer(p))[:n]
			}
			dest[i] = b

		case C.SQLITE_NULL:
			dest[i] = nil

		default:
			return fmt.Errorf("unexpected column type %v", cT)
		}
	}

	return nil
}

// result is the result of a query execution.
// result satisfies driver.Result.
type result struct {
	lastInsertID int64
	rowsAffected int64
}

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary
// key.
func (r *result) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

// RowsAffected returns the number of rows affected by the
// query.
func (r *result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
