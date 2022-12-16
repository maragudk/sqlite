package sqlite_test

import (
	"database/sql"
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
	t.Skip()

	t.Run("sets default pragmas", func(t *testing.T) {
		db := open(t, sqlite.Options{})

		tests := []struct {
			name     string
			expected string
		}{
			{name: "journal_mode", expected: "wal"},
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
}

func TestDB_Exec(t *testing.T) {
	t.Run("select 1", func(t *testing.T) {
		db := open(t, sqlite.Options{})
		_, err := db.Exec(`select 1`)
		assert.NoErr(t, err)
	})
}

func open(t *testing.T, opts sqlite.Options) *sql.DB {
	t.Helper()

	opts.Name = strconv.Itoa(int(time.Now().UnixNano()))

	sqlite.RegisterDriver(opts)

	db, err := sql.Open(opts.Name, ":memory:")
	assert.NoErr(t, err)

	return db
}
