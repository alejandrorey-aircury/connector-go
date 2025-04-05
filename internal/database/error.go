package database

import "fmt"

type ConnectionError struct {
	Message string
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("Error connecting to database: %s", e.Message)
}
