package storage_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/benkivuva/my-rdbms/internal/storage"
)

func TestStorageLayer(t *testing.T) {
	fileName := "test_storage.db"
	// Clean up previous run
	os.Remove(fileName)
	defer os.Remove(fileName)

	t.Log("Initializing DiskManager...")
	dm, err := storage.NewDiskManager(fileName)
	if err != nil {
		t.Fatalf("Failed to create DiskManager: %v", err)
	}
	defer dm.Close()

	t.Log("Initializing BufferPool...")
	bp := storage.NewBufferPool(10, dm) // 10 pages capacity

	// Create a new page
	t.Log("Creating new page...")
	p1, err := bp.NewPage()
	if err != nil {
		t.Fatalf("Failed to create new page: %v", err)
	}

	data := []byte("Hello RDBMS World!")
	// Ensure we don't overflow
    if len(data) > storage.PageSize {
        t.Fatal("Data too long")
    }
    
	t.Logf("Writing data to page %d: %s", p1.ID, data)
	p1.Copy(data)

	id := p1.ID
	bp.UnpinPage(id, true) // Mark dirty and unpin

	// Flush all to ensure it's on disk
	t.Log("Flushing all pages...")
	if err := bp.FlushAll(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Read back
	t.Logf("Fetching page %d...", id)
	p2, err := bp.FetchPage(id)
	if err != nil {
		t.Fatalf("Failed to fetch page: %v", err)
	}

	readData := p2.GetData()[:len(data)]
	t.Logf("Read data: %s", string(readData))

	if !bytes.Equal(readData, data) {
		t.Fatalf("Data mismatch! Expected %s, got %s", data, readData)
	}
}
