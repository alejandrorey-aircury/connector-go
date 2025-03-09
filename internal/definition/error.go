package definition

import "fmt"

type ProcessError struct {
	Message string
}

func (e *ProcessError) Error() string {
	return fmt.Sprintf("Error processing definition file: %s", e.Message)
}
