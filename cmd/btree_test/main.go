package main

import (
	"fmt"
	"log"
	"os"

	"github.com/benkivuva/my-rdbms/internal/index"
	"github.com/benkivuva/my-rdbms/internal/storage"
)

func main() {
	fileName := "test_btree.db"
	os.Remove(fileName)
	defer os.Remove(fileName)

	dm, _ := storage.NewDiskManager(fileName)
	bp := storage.NewBufferPool(50, dm) // Larger pool for BTree

    // Initialize Tree with new root
	bt, err := index.NewBTreeIndex(bp, 0)
    if err != nil {
        log.Fatalf("Failed to init BTree: %v", err)
    }
    
    fmt.Println("Inserting keys...")
    // Insert enough to force split
    // Leaf capacity is ~200.
    count := 300
    for i := 0; i < count; i++ {
        rid := storage.RID{PageID: storage.PageID(i), SlotID: 0}
        // Insert keys: 0, 10, 20...
        key := int64(i * 10)
        if err := bt.Insert(key, rid); err != nil {
            log.Fatalf("Insert failed at %d: %v", i, err)
        }
    }
    
    fmt.Println("Searching keys...")
    // Search check
    for i := 0; i < count; i++ {
        key := int64(i * 10)
        rid, err := bt.Search(key)
        if err != nil {
            log.Fatalf("Search failed for %d: %v", key, err)
        }
        if rid.PageID != storage.PageID(i) {
            log.Fatalf("RID mismatch for key %d: got %d want %d", key, rid.PageID, i)
        }
    }
    
    fmt.Println("B-Tree Verified Successfully!")
}
