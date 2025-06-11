package threads

import "fmt"

// Error types for the threads library.

// ErrInvalidConfig represents a configuration validation error.
type ErrInvalidConfig string

func (e ErrInvalidConfig) Error() string {
	return fmt.Sprintf("invalid configuration: %s", string(e))
}

// ErrFormat represents a file format error.
type ErrFormat struct {
	Format string
	Err    error
}

func (e ErrFormat) Error() string {
	return fmt.Sprintf("format error (%s): %v", e.Format, e.Err)
}

func (e ErrFormat) Unwrap() error {
	return e.Err
}

// ErrClosed represents an operation on a closed logger.
type ErrClosed string

func (e ErrClosed) Error() string {
	return fmt.Sprintf("logger is closed: %s", string(e))
}
