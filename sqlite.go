//go:build cgo

package sqlite

// #include <stdlib.h>
// #include <sqlite3.h>
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"unsafe"
)

func init() {
	sql.Register("sqlite", &Driver{})
}

// Driver satisfies driver.Driver.
type Driver struct{}

// Open returns a new connection to the database.
// The name is a string in a driver-specific format.
//
// Open may return a cached connection (one previously
// closed), but doing so is unnecessary; the sql package
// maintains a pool of idle connections for efficient re-use.
//
// The returned connection is only used by one goroutine at a
// time.
func (d *Driver) Open(name string) (driver.Conn, error) {
	var db *C.sqlite3

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	// The default threading mode is serialized, so no need to set explicitly: https://www.sqlite.org/threadsafe.html
	if code := C.sqlite3_open_v2(cName, &db, C.SQLITE_OPEN_READWRITE|C.SQLITE_OPEN_CREATE, nil); code != C.SQLITE_OK {
		if db != nil {
			// TODO handle return value
			C.sqlite3_close_v2(db)
		}
		return nil, fmt.Errorf("error opening connection (error code %v)", code)
	}
	return &Connection{db: db}, nil
}

// Connection satisfies driver.Conn.
type Connection struct {
	db *C.sqlite3
}

func (c *Connection) Prepare(query string) (driver.Stmt, error) {
	return &Statement{db: c.db, query: query}, nil
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
func (c *Connection) Close() error {
	if code := C.sqlite3_close_v2(c.db); code != C.SQLITE_OK {
		return fmt.Errorf("error closing connection (error code %v)", code)
	}
	C.free(unsafe.Pointer(c.db))
	c.db = nil
	return nil
}

func (c *Connection) Begin() (driver.Tx, error) {
	//TODO implement me
	panic("implement Begin")
}

// Statement satisfies driver.Stmt.
type Statement struct {
	db    *C.sqlite3
	query string
}

func (s *Statement) Close() error {
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
func (s *Statement) NumInput() int {
	return -1
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	cQuery := C.CString(s.query)
	defer C.free(unsafe.Pointer(cQuery))

	if C.sqlite3_exec(s.db, cQuery, nil, nil, nil) != C.SQLITE_OK {
		return nil, errors.New("error executing")
	}

	return nil, nil
}

// Query executes a query that may return rows, such as a
// SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (s *Statement) Query(args []driver.Value) (driver.Rows, error) {
	//TODO implement me
	panic("implement Query")
}
