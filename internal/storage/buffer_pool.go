package storage

import (
	"errors"
	"fmt"
	"sync"
)

// BufferPool manages the in-memory cache of pages.
type BufferPool struct {
	diskManager *DiskManager
	pages       map[PageID]*Page
	capacity    int
	mu          sync.Mutex
}

// NewBufferPool creates a new buffer pool with the given capacity.
func NewBufferPool(capacity int, diskManager *DiskManager) *BufferPool {
	return &BufferPool{
		diskManager: diskManager,
		pages:       make(map[PageID]*Page),
		capacity:    capacity,
	}
}

// FetchPage returns the requested page. If it is not in the buffer pool,
// it reads it from the disk manager. The returned page is pinned.
func (bp *BufferPool) FetchPage(pageID PageID) (*Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if page, ok := bp.pages[pageID]; ok {
		page.PinCount++
		return page, nil
	}

	if len(bp.pages) >= bp.capacity {
		if err := bp.evict(); err != nil {
			return nil, fmt.Errorf("buffer pool full: %w", err)
		}
	}

	page := NewPage(pageID)
	if err := bp.diskManager.ReadPage(pageID, page); err != nil {
		return nil, err
	}

	page.PinCount = 1
	bp.pages[pageID] = page
	return page, nil
}

// UnpinPage decrements the pin count of a page. If isDirty is true, the page is marked as dirty.
func (bp *BufferPool) UnpinPage(pageID PageID, isDirty bool) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if page, ok := bp.pages[pageID]; ok {
		if page.PinCount > 0 {
			page.PinCount--
		}
		if isDirty {
			page.IsDirty = true
		}
	}
}

// FlushPage writes the page to disk if it is dirty.
func (bp *BufferPool) FlushPage(pageID PageID) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.flushPage(pageID)
}

func (bp *BufferPool) flushPage(pageID PageID) error {
	if page, ok := bp.pages[pageID]; ok {
		if page.IsDirty {
			if err := bp.diskManager.WritePage(page); err != nil {
				return err
			}
			page.IsDirty = false
		}
	}
	return nil
}

// NewPage allocates a new page in the buffer pool and on disk.
func (bp *BufferPool) NewPage() (*Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if len(bp.pages) >= bp.capacity {
		if err := bp.evict(); err != nil {
			return nil, fmt.Errorf("buffer pool full: %w", err)
		}
	}

	pageID, err := bp.diskManager.AllocatePage()
	if err != nil {
		return nil, err
	}

	page := NewPage(pageID)
	page.PinCount = 1
	bp.pages[pageID] = page
	
	return page, nil
}

// evict selects a victim page to remove from the buffer pool.
// It uses a simple policy: find the first unpinned page.
func (bp *BufferPool) evict() error {
	for id, page := range bp.pages {
		if page.PinCount == 0 {
			if err := bp.flushPage(id); err != nil {
				return err
			}
			delete(bp.pages, id)
			return nil
		}
	}
	return errors.New("all pages are pinned") // No victim found
}

// FlushAll flushes all pages to disk.
func (bp *BufferPool) FlushAll() error {
    bp.mu.Lock()
    defer bp.mu.Unlock()
    
    for id := range bp.pages {
        if err := bp.flushPage(id); err != nil {
            return err
        }
    }
    return nil
}
