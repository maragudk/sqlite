package sqlite_test

import (
	"database/sql"
	"testing"

	"github.com/maragudk/sqlite"
	"github.com/maragudk/sqlite/internal/assert"
)

func TestRegisterDriver(t *testing.T) {
	t.Run("can open with default driver name sqlite", func(t *testing.T) {
		sqlite.RegisterDriver("")
		_, err := sql.Open("sqlite", ":memory:")
		assert.NoErr(t, err)
	})

	t.Run("can open with custom driver name", func(t *testing.T) {
		sqlite.RegisterDriver("foo")
		_, err := sql.Open("foo", ":memory:")
		assert.NoErr(t, err)
	})

	t.Run("errors on no register", func(t *testing.T) {
		_, err := sql.Open("bar", ":memory:")
		assert.Err(t, err)
	})
}

func TestDB_Exec(t *testing.T) {
	t.Run("select 1", func(t *testing.T) {
		sqlite.RegisterDriver("")
		db, err := sql.Open("sqlite", ":memory:")
		assert.NoErr(t, err)
		_, err = db.Exec(`select 1`)
		assert.NoErr(t, err)
	})
}
