package storage

import (
	"encoding/binary"
	"fmt"
)

// NextPageID methods for SlottedPage
func (sp *SlottedPage) GetNextPageID() PageID {
    return PageID(binary.BigEndian.Uint64(sp.page.Data[OffsetNextPageID:]))
}

func (sp *SlottedPage) SetNextPageID(pid PageID) {
    binary.BigEndian.PutUint64(sp.page.Data[OffsetNextPageID:], uint64(pid))
}

// TableHeap represents a physical table on disk, consisting of a linked list of pages.
type TableHeap struct {
	bufferPool  *BufferPool
	firstPageID PageID
}

// NewTableHeap creates or loads a table heap. 
// For a new table, firstPageID should be 0 (implied invalid or we allocate one).
func NewTableHeap(bp *BufferPool, firstPageID PageID) (*TableHeap, error) {
    th := &TableHeap{
        bufferPool: bp,
        firstPageID: firstPageID,
    }
    // If firstPageID is InvalidPageID, we need to handle it.
    // If it's a NEW table (passed InvalidPageID), we allocate.
    // However, if we are loading an existing table, firstPageID should be valid (>=0).
    // Let's assume passed InvalidPageID means "Create New".
    if th.firstPageID == InvalidPageID {
        p, err := bp.NewPage()
        if err != nil {
            return nil, err
        }
        sp := NewSlottedPage(p)
        sp.SetNextPageID(InvalidPageID) // No next page
        th.firstPageID = p.ID
        bp.UnpinPage(p.ID, true)
    }
    return th, nil
}

// InsertTuple inserts a tuple into the heap.
func (th *TableHeap) InsertTuple(data []byte) (RID, error) {
    currPageID := th.firstPageID
    
    // Find a page with space
    for {
        page, err := th.bufferPool.FetchPage(currPageID)
        if err != nil {
            return RID{}, err
        }
        sp := NewSlottedPage(page)
        
        slotID, err := sp.InsertTuple(data)
        if err == nil {
            // Success
            th.bufferPool.UnpinPage(currPageID, true)
            return RID{PageID: currPageID, SlotID: uint32(slotID)}, nil
        }
        
        // Full, try next
        nextID := sp.GetNextPageID()
        if nextID == InvalidPageID {
            // End of chain, allocate new page
            newPage, err := th.bufferPool.NewPage()
            if err != nil {
                th.bufferPool.UnpinPage(currPageID, false)
                return RID{}, err
            }
            newSP := NewSlottedPage(newPage)
            newSP.SetNextPageID(InvalidPageID)
            
            // Link
            sp.SetNextPageID(newPage.ID)
            th.bufferPool.UnpinPage(currPageID, true) // Write changed link
            
            // Insert into new page
            slotID, err := newSP.InsertTuple(data)
            if err != nil {
                th.bufferPool.UnpinPage(newPage.ID, false) // Should not happen on new page
                return RID{}, err
            }
            th.bufferPool.UnpinPage(newPage.ID, true)
            return RID{PageID: newPage.ID, SlotID: uint32(slotID)}, nil
        }
        
        th.bufferPool.UnpinPage(currPageID, false)
        currPageID = nextID
    }
}

// GetTuple retrieves a tuple by RID.
func (th *TableHeap) GetTuple(rid RID) ([]byte, error) {
    page, err := th.bufferPool.FetchPage(rid.PageID)
    if err != nil {
        return nil, err
    }
    defer th.bufferPool.UnpinPage(rid.PageID, false)
    
    sp := NewSlottedPage(page)
    data := sp.GetTuple(int(rid.SlotID))
    if data == nil {
        return nil, fmt.Errorf("tuple not found")
    }
    
    // return copy
    out := make([]byte, len(data))
    copy(out, data)
    return out, nil
}

// Iterator returns an iterator over the heap
// (To be implemented for SeqScan)
type TableIterator struct {
    tableHeap  *TableHeap
    currPageID PageID
    currSlot   int
}

func (th *TableHeap) Iterator() *TableIterator {
    return &TableIterator{
        tableHeap: th,
        currPageID: th.firstPageID,
        currSlot: 0,
    }
}

func (it *TableIterator) Next() ([]byte, RID, error) {
    for {
        if it.currPageID == InvalidPageID {
            return nil, RID{}, nil // EOF
        }
        
        page, err := it.tableHeap.bufferPool.FetchPage(it.currPageID)
        if err != nil {
            return nil, RID{}, err
        }
        sp := NewSlottedPage(page)
        numSlots := int(sp.GetNumSlots())
        
        if it.currSlot < numSlots {
            data := sp.GetTuple(it.currSlot)
            rid := RID{PageID: it.currPageID, SlotID: uint32(it.currSlot)}
            it.currSlot++
            
            if data != nil {
                 it.tableHeap.bufferPool.UnpinPage(it.currPageID, false)
                 out := make([]byte, len(data))
                 copy(out, data)
                 return out, rid, nil
            }
            // If deleted (nil), continue loop to next slot
            it.tableHeap.bufferPool.UnpinPage(it.currPageID, false)
            continue
        }
        
        // Next Page
        nextID := sp.GetNextPageID()
        it.tableHeap.bufferPool.UnpinPage(it.currPageID, false)
        it.currPageID = nextID
        it.currSlot = 0
    }
}
