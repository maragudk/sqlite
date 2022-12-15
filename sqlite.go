//go:build cgo

package sqlite

// #include <stdlib.h>
// #include <sqlite3.h>
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"unsafe"
)

func init() {
	sql.Register("sqlite", &Driver{})
}

// Driver satisfies driver.Driver.
type Driver struct {
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	var db *C.sqlite3

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	code := C.sqlite3_open_v2(cName, &db, C.SQLITE_OPEN_READWRITE|C.SQLITE_OPEN_CREATE, nil)
	if code != C.SQLITE_OK {
		if db != nil {
			C.sqlite3_close_v2(db)
		}
		return nil, errors.New("error opening database")
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

func (c *Connection) Close() error {
	//TODO implement me
	panic("implement Close")
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
	if C.sqlite3_close_v2(s.db) != C.SQLITE_OK {
		return errors.New("error closing database")
	}
	return nil
}

func (s *Statement) NumInput() int {
	return -1
}

func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	cQuery := C.CString(s.query)
	defer C.free(unsafe.Pointer(cQuery))

	if C.sqlite3_exec(s.db, cQuery, nil, nil, nil) != C.SQLITE_OK {
		return nil, errors.New("error executing")
	}

	return nil, nil
}

func (s *Statement) Query(args []driver.Value) (driver.Rows, error) {
	//TODO implement me
	panic("implement Query")
}
