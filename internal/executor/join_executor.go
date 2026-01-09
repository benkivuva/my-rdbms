package executor

import (
	"github.com/benkivuva/my-rdbms/internal/storage"
)

// NestedLoopJoinExecutor performs a simple nested loop join.
type NestedLoopJoinExecutor struct {
	leftChild        Executor
	rightHeap        *storage.TableHeap
	leftField        string
	rightField       string
	currentLeftTuple *Tuple
	rightIterator    *storage.TableIterator
	done             bool
}

// NewNestedLoopJoinExecutor creates a new nested loop join executor.
func NewNestedLoopJoinExecutor(left Executor, rightHeap *storage.TableHeap, leftField, rightField string) *NestedLoopJoinExecutor {
	return &NestedLoopJoinExecutor{
		leftChild:  left,
		rightHeap:  rightHeap,
		leftField:  leftField,
		rightField: rightField,
	}
}

func (e *NestedLoopJoinExecutor) Init() error {
	return e.leftChild.Init()
}

func (e *NestedLoopJoinExecutor) Close() error {
	return e.leftChild.Close()
}

func (e *NestedLoopJoinExecutor) Next() (*Tuple, error) {
	for {
		if e.done {
			return nil, nil
		}

		// Get left tuple if we don't have one
		if e.currentLeftTuple == nil {
			tuple, err := e.leftChild.Next()
			if err != nil {
				return nil, err
			}
			if tuple == nil {
				e.done = true
				return nil, nil
			}
			e.currentLeftTuple = tuple
			e.rightIterator = e.rightHeap.Iterator()
		}

		// Scan right table
		data, _, err := e.rightIterator.Next()
		if err != nil {
			return nil, err
		}

		if data == nil {
			// Right exhausted, move to next left
			e.currentLeftTuple = nil
			continue
		}

		rightTuple := &Tuple{Values: []interface{}{string(data)}}

		// Check join condition (simplified: compare first values)
		leftVal := e.currentLeftTuple.Values[0]
		rightVal := rightTuple.Values[0]

		if leftVal == rightVal {
			// Match! Return combined tuple
			combined := &Tuple{
				Values: append(e.currentLeftTuple.Values, rightTuple.Values...),
			}
			return combined, nil
		}
	}
}
