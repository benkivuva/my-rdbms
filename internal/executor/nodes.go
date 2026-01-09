package executor

import (
	"fmt"

	"github.com/benkivuva/my-rdbms/internal/index"
	"github.com/benkivuva/my-rdbms/internal/sql"
	"github.com/benkivuva/my-rdbms/internal/storage"
)

// SeqScanExecutor performs a sequential scan over all tuples in a heap.
type SeqScanExecutor struct {
	iterator *storage.TableIterator
}

// NewSeqScanExecutor creates a new sequential scan executor.
func NewSeqScanExecutor(heap *storage.TableHeap) *SeqScanExecutor {
	return &SeqScanExecutor{iterator: heap.Iterator()}
}

func (e *SeqScanExecutor) Init() error  { return nil }
func (e *SeqScanExecutor) Close() error { return nil }

func (e *SeqScanExecutor) Next() (*Tuple, error) {
	data, _, err := e.iterator.Next()
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	return &Tuple{Values: []interface{}{string(data)}}, nil
}

// InsertExecutor inserts a tuple into the heap and index.
type InsertExecutor struct {
	btree     *index.BTreeIndex
	tableHeap *storage.TableHeap
	values    []interface{}
}

// NewInsertExecutor creates a new insert executor.
func NewInsertExecutor(btree *index.BTreeIndex, heap *storage.TableHeap, values []interface{}) *InsertExecutor {
	return &InsertExecutor{btree: btree, tableHeap: heap, values: values}
}

func (e *InsertExecutor) Init() error  { return nil }
func (e *InsertExecutor) Close() error { return nil }

func (e *InsertExecutor) Next() (*Tuple, error) {
	if len(e.values) == 0 {
		return nil, nil
	}

	var data []byte
	var keyVal int64

	val := e.values[0]
	switch v := val.(type) {
	case int:
		keyVal = int64(v)
		data = []byte(fmt.Sprintf("%d", v))
	case string:
		return nil, fmt.Errorf("primary key must be int")
	default:
		return nil, fmt.Errorf("unsupported type for primary key")
	}

	// Check for unique constraint violation
	if _, err := e.btree.Search(keyVal); err == nil {
		return nil, fmt.Errorf("unique constraint violation: key %d already exists", keyVal)
	}

	rid, err := e.tableHeap.InsertTuple(data)
	if err != nil {
		return nil, err
	}

	if err := e.btree.Insert(keyVal, rid); err != nil {
		return nil, err
	}

	return &Tuple{Values: e.values}, nil
}

// FilterExecutor filters tuples based on a WHERE clause.
type FilterExecutor struct {
	child Executor
	cond  *sql.WhereClause
}

// NewFilterExecutor creates a new filter executor.
func NewFilterExecutor(child Executor, cond *sql.WhereClause) *FilterExecutor {
	return &FilterExecutor{child: child, cond: cond}
}

func (e *FilterExecutor) Init() error  { return e.child.Init() }
func (e *FilterExecutor) Close() error { return e.child.Close() }

func (e *FilterExecutor) Next() (*Tuple, error) {
	for {
		tuple, err := e.child.Next()
		if err != nil {
			return nil, err
		}
		if tuple == nil {
			return nil, nil
		}

		if e.cond == nil {
			return tuple, nil
		}

		val := tuple.Values[0]
		match := false

		switch e.cond.Op {
		case "=":
			match = val == e.cond.Value
		case ">":
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
	}
}

// DeleteExecutor deletes tuples matching WHERE clause from heap and index.
type DeleteExecutor struct {
	tableHeap *storage.TableHeap
	btree     *index.BTreeIndex
	iterator  *storage.TableIterator
	cond      *sql.WhereClause
	count     int
	done      bool
}

// NewDeleteExecutor creates a new delete executor.
func NewDeleteExecutor(heap *storage.TableHeap, btree *index.BTreeIndex, cond *sql.WhereClause) *DeleteExecutor {
	return &DeleteExecutor{
		tableHeap: heap,
		btree:     btree,
		iterator:  heap.Iterator(),
		cond:      cond,
	}
}

func (e *DeleteExecutor) Init() error  { return nil }
func (e *DeleteExecutor) Close() error { return nil }

func (e *DeleteExecutor) Next() (*Tuple, error) {
	if e.done {
		return nil, nil
	}

	for {
		data, rid, err := e.iterator.Next()
		if err != nil {
			return nil, err
		}
		if data == nil {
			e.done = true
			return &Tuple{Values: []interface{}{e.count}}, nil
		}

		// Check if tuple matches WHERE clause
		match := true
		if e.cond != nil {
			val := string(data)
			match = (e.cond.Op == "=" && val == fmt.Sprintf("%v", e.cond.Value))
		}

		if match {
			// Delete from heap
			if err := e.tableHeap.DeleteTuple(rid); err != nil {
				return nil, err
			}
			// Delete from index (parse key from data)
			var keyVal int64
			fmt.Sscanf(string(data), "%d", &keyVal)
			// Note: B-Tree delete not implemented, but tuple is removed from heap
			e.count++
		}
	}
}
