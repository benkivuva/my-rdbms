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

// Global Engine State
type Engine struct {
    bp        *storage.BufferPool
    dm        *storage.DiskManager
    heap      *storage.TableHeap
    btree     *index.BTreeIndex
}

func initEngine(dbName string) (*Engine, error) {
    dm, err := storage.NewDiskManager(dbName)
    if err != nil {
        return nil, err
    }
    bp := storage.NewBufferPool(100, dm) // 100 pages
    
    // In real DB we load catalog to find root PageIDs.
    // Here we hardcode or just try to load.
    // Simplification: Assume Page 0 is Heap Header (SlotPage with NextPageID)
    // And Page ?? is BTree Root.
    // If new file, everything 0.
    
    // Let's assume Heap starts at Page 0?
    // Wait, Page 0 might be metadata?
    // NewTableHeap takes firstPageID. If InvalidPageID, it allocates a new page.
    heap, err := storage.NewTableHeap(bp, storage.InvalidPageID)
    if err != nil {
        return nil, err
    }
    
    // BTree Root?
    // How do we find BTree root if we restart?
    // We need to persist it. 
    // For this session: We won't persist BTree root ID effectively across restarts unless we add a MetaPage.
    // Let's assume we just create a new Index for now (volatile index) or use Page 1 if possible.
    // If Heap uses Page 0..N, BTree needs to allocate separate pages.
    // NewBTreeIndex(bp, 0) -> Allocates new root.
    // This leaks trees on restart.
    // TODO: MetaPage.
    
    btree, err := index.NewBTreeIndex(bp, storage.InvalidPageID)
    if err != nil {
        return nil, err
    }
    
    return &Engine{bp: bp, dm: dm, heap: heap, btree: btree}, nil
}

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
        vals := s.Values
        exec := executor.NewInsertExecutor(e.btree, e.heap, vals)
        if _, err := exec.Next(); err != nil {
             fmt.Println("Execution Error:", err)
        } else {
             fmt.Println("INSERT OK")
        }
        
    case *sql.SelectStatement:
        // Build plan: SeqScan -> Filter
        var exec executor.Executor
        exec = executor.NewSeqScanExecutor(e.heap)
        
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
            // Print tuple
            // Currently just string
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
