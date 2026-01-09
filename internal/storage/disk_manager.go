package storage

import (
	"fmt"
	"io"
	"os"
	"sync"
)

// DiskManager is responsible for reading and writing pages to disk.
type DiskManager struct {
	file     *os.File
	fileName string
	mu       sync.RWMutex
}

// NewDiskManager creates or opens a database file.
func NewDiskManager(fileName string) (*DiskManager, error) {
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open db file: %w", err)
	}
	return &DiskManager{
		file:     file,
		fileName: fileName,
	}, nil
}

// Close closes the underlying file.
func (d *DiskManager) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.file.Close()
}

// AllocatePage allocates a new page on disk and returns its ID.
func (d *DiskManager) AllocatePage() (PageID, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	info, err := d.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file: %w", err)
	}
	fileSize := info.Size()
	nextPageID := PageID(fileSize / int64(PageSize))

	// Write empty data to extend the file
	emptyData := make([]byte, PageSize)
	_, err = d.file.WriteAt(emptyData, int64(nextPageID)*int64(PageSize))
	if err != nil {
		return 0, fmt.Errorf("failed to allocate page: %w", err)
	}

	return nextPageID, nil
}

// WritePage writes the page data to disk.
func (d *DiskManager) WritePage(page *Page) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	offset := int64(page.ID) * int64(PageSize)
	_, err := d.file.WriteAt(page.Data[:], offset)
	if err != nil {
		return fmt.Errorf("failed to write page %d: %w", page.ID, err)
	}
	return nil
}

// ReadPage reads the page data from disk into the provided page.
func (d *DiskManager) ReadPage(pageID PageID, page *Page) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	offset := int64(pageID) * int64(PageSize)
	
    // Ensure we read into the page's data buffer
	n, err := d.file.ReadAt(page.Data[:], offset)
	if err != nil && err != io.EOF {
		return fmt.Errorf("failed to read page %d: %w", pageID, err)
	}
	
    // Handle partial reads (e.g., if file was corrupted or weird end)
	if n < PageSize {
        // Zero out the rest of the buffer if needed, though usually ReadAt doesn't guarantee partial zeroing.
        for i := n; i < PageSize; i++ {
            page.Data[i] = 0
        }
	}
    
    page.ID = pageID
	return nil
}
