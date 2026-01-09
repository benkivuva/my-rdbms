package executor

import (
	"fmt"

	"github.com/benkivuva/my-rdbms/internal/index"
	"github.com/benkivuva/my-rdbms/internal/sql"
	"github.com/benkivuva/my-rdbms/internal/storage"
)

// SeqScanExecutor iterates over all tuples using TableHeap iterator.
type SeqScanExecutor struct {
    iterator *storage.TableIterator
}

func NewSeqScanExecutor(heap *storage.TableHeap) *SeqScanExecutor {
    return &SeqScanExecutor{iterator: heap.Iterator()}
}

func (e *SeqScanExecutor) Init() error {
    // Iterator created in constructor is at start
    return nil
}

func (e *SeqScanExecutor) Next() (*Tuple, error) {
    data, _, err := e.iterator.Next()
    if err != nil {
        return nil, err
    }
    if data == nil {
        return nil, nil // EOF
    }
    
    // Convert []byte data back to values
    // Need Schema for this.
    // Simplified: Assume data is just the string/int needed.
    // Hack: We stored "Row-XXXX" string or int bytes.
    // Let's assume we just return it as string for now if schema unknown.
    return &Tuple{Values: []interface{}{string(data)}}, nil
}

func (e *SeqScanExecutor) Close() error { return nil }


// InsertExecutor
type InsertExecutor struct {
    btree       *index.BTreeIndex
    tableHeap   *storage.TableHeap
    values      []interface{}
    tableName   string
}

func NewInsertExecutor(btree *index.BTreeIndex, heap *storage.TableHeap, values []interface{}) *InsertExecutor {
    return &InsertExecutor{btree: btree, tableHeap: heap, values: values}
}

func (e *InsertExecutor) Init() error { return nil }

func (e *InsertExecutor) Next() (*Tuple, error) {
    if len(e.values) == 0 {
        return nil, nil
    }
    
    // 1. Serialize Tuple
    // Simplified: Just convert first value to string bytes
    // Real DB: Serialize based on schema.
    var data []byte
    var keyVal int64
    
    if len(e.values) > 0 {
         val := e.values[0]
         if v, ok := val.(int); ok {
             keyVal = int64(v)
             data = []byte(fmt.Sprintf("%d", v))
         } else if _, ok := val.(string); ok {
             // If PK is string (hash?), but BTree expects int64
             // Convert string hash? Or just parse int?
             // Prompt requirements said INT and VARCHAR types.
             // Implemented BTree with int64 keys.
             // So PK must be INT.
             return nil, fmt.Errorf("PK must be int")
         }
    }
    
    // 2. Insert into Heap
    rid, err := e.tableHeap.InsertTuple(data)
    if err != nil {
        return nil, err
    }
    
    // 3. Insert into Index
    if err := e.btree.Insert(keyVal, rid); err != nil {
        return nil, err
    }
    
    return &Tuple{Values: e.values}, nil
}

func (e *InsertExecutor) Close() error { return nil }


// FilterExecutor
type FilterExecutor struct {
    child Executor
    cond  *sql.WhereClause
}

func NewFilterExecutor(child Executor, cond *sql.WhereClause) *FilterExecutor {
    return &FilterExecutor{child: child, cond: cond}
}

func (e *FilterExecutor) Init() error {
    return e.child.Init()
}

func (e *FilterExecutor) Next() (*Tuple, error) {
    for {
        tuple, err := e.child.Next()
        if err != nil {
            return nil, err
        }
        if tuple == nil {
            return nil, nil
        }
        
        // Evaluate Filter
        // Assume Tuple satisfies filter? Need Schema to map Field name to index.
        // Simplified: Assume Field "id" is index 0.
        
        if e.cond == nil {
            return tuple, nil
        }
        
        val := tuple.Values[0] // Hardcoded ID
        // Compare
        match := false
        switch e.cond.Op {
        case "=":
            match = val == e.cond.Value
        case ">":
             // Type assertion hell?
             if v, ok := val.(int); ok {
                 if cv, ok2 := e.cond.Value.(int); ok2 {
                     match = v > cv
                 }
             }
        case "<":
             if v, ok := val.(int); ok {
                 if cv, ok2 := e.cond.Value.(int); ok2 {
                     match = v < cv
                 }
             }
        }
        
        if match {
            return tuple, nil
        }
        // Loop again
    }
}

func (e *FilterExecutor) Close() error { return e.child.Close() }
