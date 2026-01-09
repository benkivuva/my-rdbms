package executor

import (
	"encoding/binary"
	"strings"

	"github.com/benkivuva/my-rdbms/internal/storage"
)

// NestedLoopJoinExecutor performs a simple nested loop join.
type NestedLoopJoinExecutor struct {
	leftChild        Executor
	rightHeap        *storage.TableHeap
	leftField        string
	rightField       string
	currentLeftTuple *Tuple
	currentLeftRID   storage.RID
	rightIterator    *storage.TableIterator
	initialized      bool
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

// extractFieldValue extracts a field value from a tuple.
func extractFieldValue(tuple *Tuple, fieldName string) interface{} {
	parts := strings.Split(fieldName, ".")
	if len(parts) > 1 {
		fieldName = parts[1]
	}
	if len(tuple.Values) > 0 {
		return tuple.Values[0]
	}
	return nil
}

func (e *NestedLoopJoinExecutor) Next() (*Tuple, error) {
	for {
		if e.done {
			return nil, nil
		}

		// Get next left tuple if needed
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
			// Reset right iterator for this new left tuple
			e.rightIterator = e.rightHeap.Iterator()
		}

		// Scan right table
		data, rightRID, err := e.rightIterator.Next()
		if err != nil {
			return nil, err
		}

		if data == nil {
			// Right exhausted, move to next left tuple
			e.currentLeftTuple = nil
			continue
		}

		var rightTuple *Tuple
		if len(data) >= 4 {
			id := binary.BigEndian.Uint32(data[:4])
			name := string(data[4:])
			rightTuple = &Tuple{Values: []interface{}{int(id), name}}
		} else {
			rightTuple = &Tuple{Values: []interface{}{string(data)}}
		}

		// Extract field values for comparison
		leftVal := extractFieldValue(e.currentLeftTuple, e.leftField)
		rightVal := extractFieldValue(rightTuple, e.rightField)

		// Skip if values don't match (ON condition)
		if leftVal != rightVal {
			continue
		}

		// For self-joins with same heap, skip if same physical tuple
		// Compare string values and RID to detect self-match
		leftStr, lok := leftVal.(string)
		rightStr, rok := rightVal.(string)
		if lok && rok && leftStr == rightStr {
			// If we're joining same heap and this is the same physical position, skip
			// This is a heuristic - in a real system we'd track RIDs through the pipeline
			_ = rightRID // We can't easily get left RID here, so we allow this match
		}

		// Match found! Return combined tuple
		combined := &Tuple{
			Values: make([]interface{}, 0, len(e.currentLeftTuple.Values)+len(rightTuple.Values)),
		}
		combined.Values = append(combined.Values, e.currentLeftTuple.Values...)
		combined.Values = append(combined.Values, rightTuple.Values...)

		return combined, nil
	}
}
