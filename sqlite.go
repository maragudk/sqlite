//go:build cgo

package sqlite

/*
#include <stdlib.h>
#include <sqlite3.h>
*/
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
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
	var db *C.sqlite3

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	// The default threading mode is serialized, but we set it explicitly: https://www.sqlite.org/threadsafe.html
	const flags = C.SQLITE_OPEN_READWRITE | C.SQLITE_OPEN_CREATE | C.SQLITE_OPEN_FULLMUTEX
	if code := C.sqlite3_open_v2(cName, &db, flags, nil); code != C.SQLITE_OK {
		if db != nil {
			// TODO handle return value
			C.sqlite3_close_v2(db)
		}
		return nil, wrapErrorCode("error opening connection", code)
	}

	c := &connection{db: db}

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

func wrapErrorCode(format string, code C.int, args ...any) error {
	args = append(args, errString(code))
	return fmt.Errorf(format+": %w", args...)
}

func errString(code C.int) error {
	return errors.New(C.GoString(C.sqlite3_errstr(code)))
}

// connection satisfies driver.Conn.
type connection struct {
	db *C.sqlite3
}

func (c *connection) Prepare(query string) (driver.Stmt, error) {
	return &statement{db: c.db, query: query}, nil
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
	if code := C.sqlite3_close_v2(c.db); code != C.SQLITE_OK {
		return wrapErrorCode("error closing connection", code)
	}
	if c.db != nil {
		c.db = nil
	}
	return nil
}

func (c *connection) Begin() (driver.Tx, error) {
	//TODO implement me
	panic("implement Begin")
}

func (c *connection) exec(format string, args ...any) error {
	query := fmt.Sprintf(format, args...)
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))
	if code := C.sqlite3_exec(c.db, cQuery, nil, nil, nil); code != C.SQLITE_OK {
		return wrapErrorCode("error running query '%v'", code, query)
	}
	return nil
}

// statement satisfies driver.Stmt.
type statement struct {
	db    *C.sqlite3
	query string
}

func (s *statement) Close() error {
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
	return -1
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (s *statement) Exec(args []driver.Value) (driver.Result, error) {
	cQuery := C.CString(s.query)
	defer C.free(unsafe.Pointer(cQuery))

	if code := C.sqlite3_exec(s.db, cQuery, nil, nil, nil); code != C.SQLITE_OK {
		return nil, wrapErrorCode("error executing", code)
	}

	return nil, nil
}

// Query executes a query that may return rows, such as a
// SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (s *statement) Query(args []driver.Value) (driver.Rows, error) {
	//TODO implement me
	panic("implement Query")
}
