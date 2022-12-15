package sqlite_test

import (
	"database/sql"
	"testing"

	_ "github.com/maragudk/sqlite"
)

func TestDriver_Open(t *testing.T) {
	t.Run("can open with driver name sqlite", func(t *testing.T) {
		_, err := sql.Open("sqlite", "app.db")
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestDB_Exec(t *testing.T) {
	t.Run("select 1", func(t *testing.T) {
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`select 1`); err != nil {
			t.Fatal(err)
		}
	})
}
