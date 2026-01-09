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

// Execute parses and executes a SQL statement.
func (e *Engine) Execute(input string) {
	l := sql.NewLexer(input)
	p, err := sql.NewParser(l)
	if err != nil {
		fmt.Println("Parser Error:", err)
		return
	}

	stmt, err := p.Parse()
	if err != nil {
		fmt.Println("Parse Error:", err)
		return
	}

	switch s := stmt.(type) {
	case *sql.InsertStatement:
		exec := executor.NewInsertExecutor(e.btree, e.heap, s.Values)
		if _, err := exec.Next(); err != nil {
			fmt.Println("Execution Error:", err)
		} else {
			fmt.Println("INSERT OK")
		}

	case *sql.SelectStatement:
		var exec executor.Executor = executor.NewSeqScanExecutor(e.heap)
		if s.Where != nil {
			exec = executor.NewFilterExecutor(exec, s.Where)
		}

		fmt.Println("----------------")
		count := 0
		for {
			tuple, err := exec.Next()
			if err != nil {
				fmt.Println("Error:", err)
				break
			}
			if tuple == nil {
				break
			}
			fmt.Printf("%v\n", tuple.Values)
			count++
		}
		fmt.Printf("(%d rows)\n", count)

	case *sql.CreateTableStatement:
		fmt.Println("CREATE TABLE: Tables are implicit in this simple engine.")

	default:
		fmt.Println("Statement not fully supported yet")
	}
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
		engine.Execute(input)
	}
}
