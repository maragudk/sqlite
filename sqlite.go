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
	"unsafe"
)

func RegisterDriver(name string) {
	if name == "" {
		name = "sqlite"
	}
	sql.Register(name, &d{})
}

// d satisfies driver.Driver.
type d struct{}

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
	return &connection{db: db}, nil
}

func wrapErrorCode(message string, code C.int) error {
	return fmt.Errorf(message+": %w", errString(code))
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
