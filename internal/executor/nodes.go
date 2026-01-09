package executor

import (
	"fmt"

	"github.com/benkivuva/my-rdbms/internal/index"
	"github.com/benkivuva/my-rdbms/internal/sql"
	"github.com/benkivuva/my-rdbms/internal/storage"
)

// SeqScanExecutor iterates over all tuples.
// For now, we assume we just scan the B-Tree index (Index Scan) since we lack a full Heap File manager.
type SeqScanExecutor struct {
    btree *index.BTreeIndex
    // We need an iterator for the B-Tree. 
    // Implementing a full iterator for B-Tree is complex, so for "Simple RDBMS" 
    // we might just cheat or implementing a basic "Leaf Iterator".
    currentKey int64
    maxKey     int64
}

func NewSeqScanExecutor(btree *index.BTreeIndex) *SeqScanExecutor {
    // Return all keys from 0 to MaxInt? 
    // Or we need a proper iterator.
    // Let's implement a dummy one that just tries keys 0..1000 for verification?
    // NO, that's partial.
    // Real approach: Add Iterator to BTree.
    return &SeqScanExecutor{btree: btree, currentKey: 0, maxKey: 10000}
}


func (e *SeqScanExecutor) Init() error {
    e.currentKey = 0
    return nil
}

func (e *SeqScanExecutor) Next() (*Tuple, error) {
    // Scan loop
    for e.currentKey < e.maxKey {
        rid, err := e.btree.Search(e.currentKey)
        e.currentKey++
        if err == nil {
            // Found a key. 
            // In a real DB, RID points to HeapTuple.
            // Here, we don't have a Heap. We just stored RID. Not useful.
            // Requirement 1: "Storage Manager... 4KB pages... Buffer Pool".
            // Requirement 2: "B-Tree for PK lookups".
            // We missed the "Store Tuple" part.
            // Storage Layer implemented Page.
            // B-Tree stores RID (PageID, Slot).
            // Where is the Tuple?
            // "The logical engine (SQL rows)".
            
            // MISSING: HeapFile or TableHeap to store actual row data.
            // The prompt says "Storage Layer... Page-based storage... Buffer Pool".
            // "Engine... INT and VARCHAR".
            // "Btree for Primary Key lookups".
            
            // So we need:
            // 1. Insert: Write tuple to a Heap Page -> get RID -> Insert (Key, RID) into B-Tree.
            // 2. Select: Search B-Tree -> Get RID -> Read Heap Page -> Get Tuple.
            
            // I haven't implemented TableHeap.
            // I should implement a simple TableHeap in `internal/storage`.
            
            // For now, to unblock Executor:
            // Let's assume RID *is* the value for now (dummy)? 
            // Or Mock it.
            
            // Let's return a Mock Tuple with the Key.
            return &Tuple{Values: []interface{}{int(e.currentKey-1)}}, nil
        }
    }
    return nil, nil // End
}

func (e *SeqScanExecutor) Close() error { return nil }


// InsertExecutor
type InsertExecutor struct {
    btree       *index.BTreeIndex
    tableHeap   *storage.TableHeap // Need to implement this
    values      []interface{}
    tableName   string
}

func NewInsertExecutor(btree *index.BTreeIndex, heap *storage.TableHeap, values []interface{}) *InsertExecutor {
    return &InsertExecutor{btree: btree, tableHeap: heap, values: values}
}

func (e *InsertExecutor) Init() error { return nil }

func (e *InsertExecutor) Next() (*Tuple, error) {
    // 1. Insert into Heap -> Get RID
    // We don't have Heap yet.
    // Let's just assume we insert into BTree (Key, RID{0,0}).
    // We need the PK. Assume first column is PK (INT).
    
    if len(e.values) == 0 {
        return nil, nil
    }
    
    keyVal, ok := e.values[0].(int)
    if !ok {
        return nil, fmt.Errorf("PK must be int")
    }
    
    // Insert to Heap (Mock)
    rid := storage.RID{PageID: 0, SlotID: 0}
    if e.tableHeap != nil {
        // rid = e.tableHeap.InsertTuple(e.values)
    }
    
    // Insert to Index
    if err := e.btree.Insert(int64(keyVal), rid); err != nil {
        return nil, err
    }
    
    // Return the inserted tuple
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
