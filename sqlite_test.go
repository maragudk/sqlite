package sqlite_test

import (
	"database/sql"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/maragudk/sqlite"
	"github.com/maragudk/sqlite/internal/assert"
)

func TestRegisterDriver(t *testing.T) {
	t.Run("can open with default driver name sqlite", func(t *testing.T) {
		sqlite.RegisterDriver(sqlite.Options{})
		_, err := sql.Open("sqlite", ":memory:")
		assert.NoErr(t, err)
	})

	t.Run("can open with custom driver name", func(t *testing.T) {
		sqlite.RegisterDriver(sqlite.Options{Name: "foo"})
		_, err := sql.Open("foo", ":memory:")
		assert.NoErr(t, err)
	})

	t.Run("errors on no register", func(t *testing.T) {
		_, err := sql.Open("bar", ":memory:")
		assert.Err(t, err)
	})
}

func TestDB_Open(t *testing.T) {
	t.Run("sets default pragmas", func(t *testing.T) {
		db := open(t, sqlite.Options{})

		tests := []struct {
			name     string
			expected string
		}{
			{name: "journal_mode", expected: "wal"},
			{name: "busy_timeout", expected: "5000"},
			{name: "foreign_keys", expected: "1"},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				var actual string
				err := db.QueryRow(`pragma ` + test.name).Scan(&actual)
				assert.NoErr(t, err)
				assert.Equal(t, test.expected, actual)
			})
		}
	})

	t.Run("can set different journal mode", func(t *testing.T) {
		db := open(t, sqlite.Options{
			JournalMode: sqlite.JournalModeTruncate,
		})

		var actual string
		err := db.QueryRow(`pragma journal_mode`).Scan(&actual)
		assert.NoErr(t, err)
		assert.Equal(t, sqlite.JournalModeTruncate.String(), actual)
	})
}

func TestDB_QueryRow(t *testing.T) {
	t.Run("select true, 1, 1.1, 'foo', 'foo'", func(t *testing.T) {
		db := open(t, sqlite.Options{})

		var b bool
		var i int
		var f float64
		var s string
		var d []byte
		err := db.QueryRow(`select true, 1, 1.1, 'foo', 'foo'`).Scan(&b, &i, &f, &s, &d)

		assert.NoErr(t, err)
		assert.Equal(t, true, b)
		assert.Equal(t, 1, i)
		assert.Equal(t, 1.1, f)
		assert.Equal(t, "foo", s)
		assert.EqualBytes(t, []byte("foo"), d)
	})

	t.Run("select true, 1, 1.1, 'foo', 'foo' with args", func(t *testing.T) {
		db := open(t, sqlite.Options{})

		var b bool
		var i int
		var f float64
		var s string
		var d []byte
		err := db.QueryRow(`select ?, ?, ?, ?, ?`, true, 1, 1.1, "foo", []byte("foo")).
			Scan(&b, &i, &f, &s, &d)

		assert.NoErr(t, err)
		assert.Equal(t, true, b)
		assert.Equal(t, 1, i)
		assert.Equal(t, 1.1, f)
		assert.Equal(t, "foo", s)
		assert.EqualBytes(t, []byte("foo"), d)
	})

	t.Run("queries an inserted and updated row from a table", func(t *testing.T) {
		db := open(t, sqlite.Options{})

		_, err := db.Exec(`create table t (v int not null)`)
		assert.NoErr(t, err)

		_, err = db.Exec(`insert into t values (?)`, 1)
		assert.NoErr(t, err)

		var v int
		err = db.QueryRow(`select * from t`).Scan(&v)
		assert.NoErr(t, err)
		assert.Equal(t, 1, v)

		_, err = db.Exec(`update t set v = ?`, 2)
		assert.NoErr(t, err)

		err = db.QueryRow(`select * from t`).Scan(&v)
		assert.NoErr(t, err)
		assert.Equal(t, 2, v)
	})
}

func open(t *testing.T, opts sqlite.Options) *sql.DB {
	t.Helper()

	opts.Name = strconv.Itoa(int(time.Now().UnixNano()))

	sqlite.RegisterDriver(opts)

	db, err := sql.Open(opts.Name, path.Join(t.TempDir(), "app.db"))
	assert.NoErr(t, err)

	return db
}
