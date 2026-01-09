package sql

// StatementType represents the type of SQL statement.
type StatementType int

const (
	StmtCreate StatementType = iota
	StmtInsert
	StmtSelect
	StmtDelete
	StmtUpdate
)

type Statement interface {
	Type() StatementType
}

type FieldType int

const (
	TypeInt FieldType = iota
	TypeVarchar
)

type ColumnDef struct {
	Name string
	Type FieldType
}

// CreateTableStatement: CREATE TABLE <name> (col1 type, col2 type)
type CreateTableStatement struct {
	TableName string
	Columns   []ColumnDef
}

func (s *CreateTableStatement) Type() StatementType { return StmtCreate }

// InsertStatement: INSERT INTO <name> VALUES (...)
type InsertStatement struct {
	TableName string
	Values    []interface{}
}

func (s *InsertStatement) Type() StatementType { return StmtInsert }

// JoinClause represents a JOIN ... ON ... clause
type JoinClause struct {
	JoinTable    string
	OnLeftField  string
	OnRightField string
}

// SelectStatement: SELECT * FROM <name> [JOIN table ON ...] [WHERE ...]
type SelectStatement struct {
	TableName string
	Fields    []string
	Join      *JoinClause
	Where     *WhereClause
}

type WhereClause struct {
	Field string
	Op    string
	Value interface{}
}

func (s *SelectStatement) Type() StatementType { return StmtSelect }

// DeleteStatement: DELETE FROM <name> WHERE ...
type DeleteStatement struct {
	TableName string
	Where     *WhereClause
}

func (s *DeleteStatement) Type() StatementType { return StmtDelete }

// SetClause represents a SET col = val assignment
type SetClause struct {
	Column string
	Value  interface{}
}

// UpdateStatement: UPDATE <name> SET col=val WHERE ...
type UpdateStatement struct {
	TableName string
	Sets      []SetClause
	Where     *WhereClause
}

func (s *UpdateStatement) Type() StatementType { return StmtUpdate }
