package sql

// StatementType represents the type of SQL statement.
type StatementType int

const (
	StmtCreate StatementType = iota
	StmtInsert
	StmtSelect
	StmtDelete
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
	Values    []interface{} // int or string
}

func (s *InsertStatement) Type() StatementType { return StmtInsert }

// SelectStatement: SELECT * FROM <name> WHERE <col> <op> <val>
// Simplified: Only supports * and single condition WHERE
type SelectStatement struct {
	TableName string
	Fields    []string // For now only ["*"] or specific fields
	Where     *WhereClause
}

type WhereClause struct {
	Field string
	Op    string // =, <, >
	Value interface{}
}

func (s *SelectStatement) Type() StatementType { return StmtSelect }

// DeleteStatement: DELETE FROM <name> WHERE ...
type DeleteStatement struct {
	TableName string
	Where     *WhereClause
}

func (s *DeleteStatement) Type() StatementType { return StmtDelete }
