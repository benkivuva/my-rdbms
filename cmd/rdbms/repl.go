package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/benkivuva/my-rdbms/internal/executor"
	"github.com/benkivuva/my-rdbms/internal/index"
	"github.com/benkivuva/my-rdbms/internal/sql"
	"github.com/benkivuva/my-rdbms/internal/storage"
)

// Engine holds the core database components.
type Engine struct {
	bp    *storage.BufferPool
	dm    *storage.DiskManager
	heap  *storage.TableHeap
	btree *index.BTreeIndex
}

// initEngine initializes the database engine with the given file.
func initEngine(dbName string) (*Engine, error) {
	dm, err := storage.NewDiskManager(dbName)
	if err != nil {
		return nil, err
	}
	bp := storage.NewBufferPool(100, dm)

	heap, err := storage.NewTableHeap(bp, storage.InvalidPageID)
	if err != nil {
		return nil, err
	}

	btree, err := index.NewBTreeIndex(bp, storage.InvalidPageID)
	if err != nil {
		return nil, err
	}

	return &Engine{bp: bp, dm: dm, heap: heap, btree: btree}, nil
}

// Execute parses and executes a SQL statement, returning the result as a string.
func (e *Engine) Execute(input string) string {
	var out strings.Builder

	l := sql.NewLexer(input)
	p, err := sql.NewParser(l)
	if err != nil {
		return fmt.Sprintf("Parser Error: %v\n", err)
	}

	stmt, err := p.Parse()
	if err != nil {
		return fmt.Sprintf("Parse Error: %v\n", err)
	}

	switch s := stmt.(type) {
	case *sql.InsertStatement:
		exec := executor.NewInsertExecutor(e.btree, e.heap, s.Values)
		if _, err := exec.Next(); err != nil {
			out.WriteString(fmt.Sprintf("Execution Error: %v\n", err))
		} else {
			out.WriteString("INSERT OK\n")
		}

	case *sql.SelectStatement:
		var exec executor.Executor = executor.NewSeqScanExecutor(e.heap)

		// Handle JOIN
		if s.Join != nil {
			exec = executor.NewNestedLoopJoinExecutor(exec, e.heap, s.Join.OnLeftField, s.Join.OnRightField)
		}

		if s.Where != nil {
			exec = executor.NewFilterExecutor(exec, s.Where)
		}

		out.WriteString("----------------\n")
		count := 0
		for {
			tuple, err := exec.Next()
			if err != nil {
				out.WriteString(fmt.Sprintf("Error: %v\n", err))
				break
			}
			if tuple == nil {
				break
			}
			out.WriteString(fmt.Sprintf("%v\n", tuple.Values))
			count++
		}
		out.WriteString(fmt.Sprintf("(%d rows)\n", count))

	case *sql.DeleteStatement:
		exec := executor.NewDeleteExecutor(e.heap, e.btree, s.Where)
		tuple, err := exec.Next()
		if err != nil {
			out.WriteString(fmt.Sprintf("Execution Error: %v\n", err))
		} else if tuple != nil {
			out.WriteString(fmt.Sprintf("DELETE %v rows\n", tuple.Values[0]))
		}

	case *sql.UpdateStatement:
		out.WriteString("UPDATE: Not fully implemented yet (use DELETE + INSERT)\n")

	case *sql.CreateTableStatement:
		out.WriteString("CREATE TABLE: Tables are implicit in this simple engine.\n")

	default:
		out.WriteString("Statement not fully supported yet\n")
	}

	return out.String()
}

// runREPL starts an interactive SQL shell.
func runREPL(engine *Engine) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Simple RDBMS REPL")
	fmt.Println("Type 'exit' to quit.")

	for {
		fmt.Print("db> ")
		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())
		if input == "exit" {
			break
		}
		if input == "" {
			continue
		}
		fmt.Print(engine.Execute(input))
	}
}
