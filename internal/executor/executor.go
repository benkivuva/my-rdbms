package executor

// Tuple represents a single row of data.
// In a real DB, this would hold values + schema.
// Here we just hold []interface{} for simplicity.
type Tuple struct {
	Values []interface{}
}

// Executor interface (Volcano Model)
type Executor interface {
	Init() error
	Next() (*Tuple, error)
	Close() error
}
