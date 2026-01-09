package sql

import (
	"fmt"
	"strings"
	"unicode"
)

type TokenType int

const (
	TokenKeyword TokenType = iota
	TokenIdentifier
	TokenLiteral // "string" or 123
	TokenSymbol  // = , ( ) *
	TokenEOF
)

type Token struct {
	Type  TokenType
	Value string
}

type Lexer struct {
	input string
	pos   int
}

func NewLexer(input string) *Lexer {
	return &Lexer{input: input}
}

func (l *Lexer) NextToken() (Token, error) {
	l.skipWhitespace()
	if l.pos >= len(l.input) {
		return Token{Type: TokenEOF}, nil
	}

	ch := l.input[l.pos]

	if isAlpha(ch) {
		return l.scanIdentifier()
	}
	if isDigit(ch) {
		return l.scanNumber()
	}
	if ch == '"' || ch == '\'' {
		return l.scanString(ch)
	}

	l.pos++
	return Token{Type: TokenSymbol, Value: string(ch)}, nil
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) && unicode.IsSpace(rune(l.input[l.pos])) {
		l.pos++
	}
}

func (l *Lexer) scanIdentifier() (Token, error) {
	start := l.pos
	for l.pos < len(l.input) && (isAlpha(l.input[l.pos]) || isDigit(l.input[l.pos]) || l.input[l.pos] == '_') {
		l.pos++
	}
	val := l.input[start:l.pos]
	// Check keywords
	switch strings.ToUpper(val) {
	case "CREATE", "TABLE", "INSERT", "INTO", "VALUES", "SELECT", "FROM", "WHERE", "DELETE", "AND", "INT", "VARCHAR":
		return Token{Type: TokenKeyword, Value: strings.ToUpper(val)}, nil
	}
	return Token{Type: TokenIdentifier, Value: val}, nil
}

func (l *Lexer) scanNumber() (Token, error) {
	start := l.pos
	for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
		l.pos++
	}
	return Token{Type: TokenLiteral, Value: l.input[start:l.pos]}, nil
}

func (l *Lexer) scanString(quote byte) (Token, error) {
	l.pos++ // skip start quote
	start := l.pos
	for l.pos < len(l.input) && l.input[l.pos] != quote {
		l.pos++
	}
	if l.pos >= len(l.input) {
		return Token{}, fmt.Errorf("unterminated string")
	}
	val := l.input[start:l.pos]
	l.pos++ // skip end quote
	return Token{Type: TokenLiteral, Value: val}, nil
}

func isAlpha(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}
