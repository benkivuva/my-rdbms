package sql

import (
	"fmt"
	"strconv"
)

type Parser struct {
	lexer     *Lexer
	curToken  Token
	peekToken Token
}

func NewParser(l *Lexer) (*Parser, error) {
	p := &Parser{lexer: l}
	// Read two tokens to setup cur and peek
	t, err := l.NextToken()
	if err != nil {
		return nil, err
	}
	p.curToken = t
	t, err = l.NextToken()
	if err != nil {
		return nil, err
	}
	p.peekToken = t
	return p, nil
}

func (p *Parser) nextToken() error {
	p.curToken = p.peekToken
	t, err := p.lexer.NextToken()
	if err != nil {
		return err
	}
	p.peekToken = t
	return nil
}

func (p *Parser) Parse() (Statement, error) {
	if p.curToken.Type == TokenKeyword {
		switch p.curToken.Value {
		case "CREATE":
			return p.parseCreate()
		case "INSERT":
			return p.parseInsert()
		case "SELECT":
			return p.parseSelect()
		case "DELETE":
			return p.parseDelete()
		}
	}
	return nil, fmt.Errorf("unexpected token %v", p.curToken)
}

// CREATE TABLE name (col type, ...)
func (p *Parser) parseCreate() (*CreateTableStatement, error) {
	if err := p.expectPeek(TokenKeyword, "TABLE"); err != nil {
		return nil, err
	}
	if err := p.expectPeek(TokenIdentifier, ""); err != nil {
		return nil, err
	}
	tableName := p.curToken.Value

	if err := p.expectPeek(TokenSymbol, "("); err != nil {
		return nil, err
	}

	cols := []ColumnDef{}
	for p.curToken.Value != ")" { 
        // We are at '(' or ',' here usually? No, expectPeek advances.
        // After '(', we expect Identifier (Column Name).
		if err := p.nextToken(); err != nil {
            return nil, err
        }
        if p.curToken.Type == TokenSymbol && p.curToken.Value == ")" {
            break
        }
        if p.curToken.Type != TokenIdentifier {
            return nil, fmt.Errorf("expected column name, got %v", p.curToken)
        }
        colName := p.curToken.Value
        
        if err := p.expectPeek(TokenKeyword, ""); err != nil {
            return nil, fmt.Errorf("expected type")
        }
        typeStr := p.curToken.Value
        var fieldType FieldType
        if typeStr == "INT" {
            fieldType = TypeInt
        } else if typeStr == "VARCHAR" {
            fieldType = TypeVarchar
        } else {
            return nil, fmt.Errorf("unknown type %s", typeStr)
        }
        
        cols = append(cols, ColumnDef{Name: colName, Type: fieldType})

        // Next should be ',' or ')'
        if p.peekToken.Value == "," {
            p.nextToken()
        } else if p.peekToken.Value != ")" {
             return nil, fmt.Errorf("expected , or ) got %v", p.peekToken)
        }
	}
    // Consume ')'
     if p.curToken.Value != ")" {
         p.nextToken()
     }

	return &CreateTableStatement{TableName: tableName, Columns: cols}, nil
}

// INSERT INTO name VALUES (v1, v2)
func (p *Parser) parseInsert() (*InsertStatement, error) {
	if err := p.expectPeek(TokenKeyword, "INTO"); err != nil {
		return nil, err
	}
	if err := p.expectPeek(TokenIdentifier, ""); err != nil {
		return nil, err
	}
	tableName := p.curToken.Value

	if err := p.expectPeek(TokenKeyword, "VALUES"); err != nil {
		return nil, err
	}
	if err := p.expectPeek(TokenSymbol, "("); err != nil {
		return nil, err
	}

	values := []interface{}{}
	for {
		if err := p.nextToken(); err != nil {
            return nil, err
        }
        if p.curToken.Value == ")" {
            break
        }
        
        // Parse Value
        if p.curToken.Type == TokenLiteral {
            // Check if int or string
            if val, err := strconv.ParseInt(p.curToken.Value, 10, 64); err == nil {
                values = append(values, int(val)) // Use int
            } else {
                values = append(values, p.curToken.Value)
            }
        } else {
            return nil, fmt.Errorf("expected literal, got %v", p.curToken)
        }
        
        if p.peekToken.Value == "," {
            p.nextToken()
        } else if p.peekToken.Value != ")" {
             return nil, fmt.Errorf("expected , or )")
        }
	}
     if p.curToken.Value != ")" {
         p.nextToken()
     }

	return &InsertStatement{TableName: tableName, Values: values}, nil
}

// SELECT * FROM name WHERE ...
func (p *Parser) parseSelect() (*SelectStatement, error) {
	// Fields
    fields := []string{}
    p.nextToken() // Skip SELECT
    
    // Parse fields until FROM
    for p.curToken.Value != "FROM" && p.curToken.Type != TokenEOF {
        fields = append(fields, p.curToken.Value) // Could be "*"
        if p.peekToken.Value == "," {
            p.nextToken()
        }
        p.nextToken()
    }
    
    if p.curToken.Value != "FROM" {
        return nil, fmt.Errorf("expected FROM")
    }
    
    if err := p.expectPeek(TokenIdentifier, ""); err != nil {
        return nil, err
    }
    tableName := p.curToken.Value
    
    var where *WhereClause
    if p.peekToken.Value == "WHERE" {
        p.nextToken() 
        w, err := p.parseWhere()
        if err != nil {
            return nil, err
        }
        where = w
    }
    
    return &SelectStatement{TableName: tableName, Fields: fields, Where: where}, nil
}

// DELETE FROM name WHERE ...
func (p *Parser) parseDelete() (*DeleteStatement, error) {
    if err := p.expectPeek(TokenKeyword, "FROM"); err != nil {
        return nil, err
    }
    if err := p.expectPeek(TokenIdentifier, ""); err != nil {
        return nil, err
    }
    tableName := p.curToken.Value
     
    var where *WhereClause
    if p.peekToken.Value == "WHERE" {
        p.nextToken() 
        w, err := p.parseWhere()
        if err != nil {
            return nil, err
        }
        where = w
    }
    return &DeleteStatement{TableName: tableName, Where: where}, nil
}

func (p *Parser) parseWhere() (*WhereClause, error) {
    // Identifier Op Value
    if err := p.expectPeek(TokenIdentifier, ""); err != nil {
        return nil, err
    }
    field := p.curToken.Value
    
    if err := p.nextToken(); err != nil {
        return nil, err
    }
    op := p.curToken.Value // =, <, >
    
    if err := p.nextToken(); err != nil {
        return nil, err
    }
    valStr := p.curToken.Value
    var val interface{}
    if v, err := strconv.ParseInt(valStr, 10, 64); err == nil {
        val = int(v)
    } else {
        val = valStr
    }
    
    return &WhereClause{Field: field, Op: op, Value: val}, nil
}

func (p *Parser) expectPeek(t TokenType, val string) error {
	if p.peekToken.Type != t {
		return fmt.Errorf("expected token type %v, got %v", t, p.peekToken.Type)
	}
	if val != "" && p.peekToken.Value != val {
		return fmt.Errorf("expected token val %s, got %s", val, p.peekToken.Value)
	}
	return p.nextToken()
}
