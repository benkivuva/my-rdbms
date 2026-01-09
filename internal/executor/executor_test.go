package executor_test

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
    
    "github.com/benkivuva/my-rdbms/internal/index"
    "github.com/benkivuva/my-rdbms/internal/storage"
    "github.com/benkivuva/my-rdbms/internal/executor"
)

func TestExecutorIntegration(t *testing.T) {
    // Use temp file for DB to avoid permission/locking issues
    f, err := os.CreateTemp("", "test_exec_*.db")
    if err != nil {
        t.Fatal(err)
    }
    fileName := f.Name()
    f.Close()
    os.Remove(fileName) // Let DiskManager create it fresh
    
    defer os.Remove(fileName)
    
    dm, err := storage.NewDiskManager(fileName)
    if err != nil {
        t.Fatalf("Failed to create DiskManager: %v", err)
    }
    // Ensure cleanup of DM (closing file)
    defer dm.Close()
    
    bp := storage.NewBufferPool(50, dm)
    
    // Init Heap and Index
    heap, err := storage.NewTableHeap(bp, storage.InvalidPageID)
    if err != nil {
        t.Fatalf("Failed to create TableHeap: %v", err)
    }
    btree, err := index.NewBTreeIndex(bp, storage.InvalidPageID)
    if err != nil {
        t.Fatalf("Failed to create BTreeIndex: %v", err)
    }
    
    // Insert Executor
    values := []interface{}{123} // Tuple (123)
    insertExec := executor.NewInsertExecutor(btree, heap, values)
    tuple, err := insertExec.Next()
    if err != nil {
        t.Fatalf("Insert failed: %v", err)
    }
    if tuple == nil {
        t.Fatal("Insert returned nil tuple")
    }
    
    // Verify Insert
    // Scan Executor
    scanExec := executor.NewSeqScanExecutor(heap)
    scanTuple, err := scanExec.Next()
    if err != nil {
        t.Fatalf("Scan failed: %v", err)
    }
    if scanTuple == nil {
        t.Fatal("Scan returned empty")
    }
    
    // Check value
    // Note: SeqScanExecutor currently returns string values due to our hack in nodes.go
    // "123"
    t.Logf("Scanned Tuple: %v", scanTuple.Values)
    val := fmt.Sprint(scanTuple.Values[0])
    if val != "123" {
        t.Errorf("Expected 123, got %s", val)
    }
    
    // Try to find using Index Search (Manual check)
    rid, err := btree.Search(123)
    if err != nil {
        t.Fatalf("Index Search failed: %v", err)
    }
    
    // Fetch from heap using RID
    data, err := heap.GetTuple(rid)
    if err != nil {
        t.Fatalf("Heap GetTuple failed: %v", err)
    }
    
    if len(data) < 4 {
        t.Fatalf("Expected at least 4 bytes of data, got %d", len(data))
    }
    
    idFromHeap := binary.BigEndian.Uint32(data[:4])
    if idFromHeap != 123 {
        t.Errorf("Heap ID mismatch: expected 123, got %d", idFromHeap)
    }
}
