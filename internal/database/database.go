package database

import (
	"database/sql"
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
