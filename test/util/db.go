package util

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-binlog/diff"
)

// DBConfig is the DB configuration.
type DBConfig struct {
	Host string `toml:"host" json:"host"`

	User string `toml:"user" json:"user"`

	Password string `toml:"password" json:"password"`

	Name string `toml:"name" json:"name"`

	Port int `toml:"port" json:"port"`
}

func (c *DBConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("DBConfig(%+v)", *c)
}

// CreateDB create a mysql fd
func CreateDB(cfg DBConfig) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return db, nil
}

// CloseDB close the mysql fd
func CloseDB(db *sql.DB) error {
	return errors.Trace(db.Close())
}

// CheckSyncState check if srouceDB and targetDB has the same table and data
func CheckSyncState(sourceDB, targetDB *sql.DB) bool {
	d := diff.New(sourceDB, targetDB)
	ok, err := d.Equal()
	if err != nil {
		log.Fatal(err)
	}

	return ok
}
