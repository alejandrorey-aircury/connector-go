package database

import (
	"database/sql"

	_ "github.com/lib/pq"
)

func ConnectDatabase(connectionStr string) (*sql.DB, error) {
	db, err := sql.Open("postgres", connectionStr)

	if err != nil {
		return nil, &ConnectionError{err.Error()}
	}

	err = db.Ping()

	if err != nil {
		return nil, &ConnectionError{err.Error()}
	}

	return db, nil
}
