package storage_test

import (
	"bytes"
	"os"
	"testing"
    "fmt"

	"github.com/benkivuva/my-rdbms/internal/storage"
)

func TestTableHeap(t *testing.T) {
	fileName := "test_heap.db"
	os.Remove(fileName)
	defer os.Remove(fileName)

	dm, _ := storage.NewDiskManager(fileName)
	bp := storage.NewBufferPool(10, dm) 

    fmt.Println("New TableHeap...")
    th, err := storage.NewTableHeap(bp, storage.InvalidPageID)
    if err != nil {
        t.Fatalf("Failed NewTableHeap: %v", err)
    }
    
    // Insert many tuples to force new pages
    count := 2000 // A tuple is small, but 2000 might fill a few pages.
    // Tuple size = 10 bytes? 2000 * 10 = 20KB = ~5 pages.
    
    for i := 0; i < count; i++ {
        data := []byte(fmt.Sprintf("Row-%04d", i))
        rid, err := th.InsertTuple(data)
        if err != nil {
            t.Fatalf("Insert failed at %d: %v", i, err)
        }
        if rid.PageID == storage.InvalidPageID {
             t.Fatalf("Invalid RID page")
        }
    }
    
    // Verify by GetTuple
    for i := 0; i < count; i++ {
         // We don't know RID mapping unless we kept it.
         // Let's use Iterator to read all and check count.
    }
    
    // Test Iterator
    fmt.Println("Scanning TableHeap...")
    it := th.Iterator()
    readCount := 0
    for {
        data, _, err := it.Next()
        if err != nil {
             t.Fatalf("Iterator error: %v", err)
        }
        if data == nil {
            break // Done
        }
        readCount++
        
        // Verify Content roughly?
        // Checking sorted order not guaranteed unless insert order is preserved by heap (it is append-only ish).
        // Expect "Row-XXXX"
    }
    
    if readCount != count {
        t.Errorf("Expected %d rows, got %d", count, readCount)
    }
}

func TestSlottedPage(t *testing.T) {
     // Unit test for slotted page specific edge cases if needed
     // e.g. fragmentation logic (not implemented), simple insert/get
     
    p := storage.NewPage(1)
    sp := storage.NewSlottedPage(p)
    sp.SetNextPageID(storage.InvalidPageID)
    
    data := []byte("hello")
    idx, err := sp.InsertTuple(data)
    if err != nil {
        t.Fatal(err)
    }
    
    read := sp.GetTuple(idx)
    if !bytes.Equal(read, data) {
        t.Fatal("Mismatch")
    }
}
