package storage

import (
	"encoding/binary"
	"fmt"
)

// GetNextPageID returns the next page ID in the linked list.
func (sp *SlottedPage) GetNextPageID() PageID {
	return PageID(int64(binary.BigEndian.Uint64(sp.page.Data[OffsetNextPageID:])))
}

// SetNextPageID sets the next page ID in the linked list.
func (sp *SlottedPage) SetNextPageID(pid PageID) {
	binary.BigEndian.PutUint64(sp.page.Data[OffsetNextPageID:], uint64(pid))
}

// TableHeap manages a collection of slotted pages as a linked list.
type TableHeap struct {
	bufferPool  *BufferPool
	firstPageID PageID
}

// NewTableHeap creates a new table heap or loads an existing one.
// Pass InvalidPageID to create a new heap.
func NewTableHeap(bp *BufferPool, firstPageID PageID) (*TableHeap, error) {
	th := &TableHeap{
		bufferPool:  bp,
		firstPageID: firstPageID,
	}

	if th.firstPageID == InvalidPageID {
		p, err := bp.NewPage()
		if err != nil {
			return nil, err
		}
		sp := NewSlottedPage(p)
		sp.SetNextPageID(InvalidPageID)
		th.firstPageID = p.ID
		bp.UnpinPage(p.ID, true)
	}
	return th, nil
}

// InsertTuple inserts a tuple into the heap and returns its RID.
func (th *TableHeap) InsertTuple(data []byte) (RID, error) {
	currPageID := th.firstPageID

	for {
		page, err := th.bufferPool.FetchPage(currPageID)
		if err != nil {
			return RID{}, err
		}
		sp := NewSlottedPage(page)

		slotID, err := sp.InsertTuple(data)
		if err == nil {
			th.bufferPool.UnpinPage(currPageID, true)
			return RID{PageID: currPageID, SlotID: uint32(slotID)}, nil
		}

		nextID := sp.GetNextPageID()
		if nextID == InvalidPageID {
			newPage, err := th.bufferPool.NewPage()
			if err != nil {
				th.bufferPool.UnpinPage(currPageID, false)
				return RID{}, err
			}
			newSP := NewSlottedPage(newPage)
			newSP.SetNextPageID(InvalidPageID)

			sp.SetNextPageID(newPage.ID)
			th.bufferPool.UnpinPage(currPageID, true)

			slotID, err := newSP.InsertTuple(data)
			if err != nil {
				th.bufferPool.UnpinPage(newPage.ID, false)
				return RID{}, err
			}
			th.bufferPool.UnpinPage(newPage.ID, true)
			return RID{PageID: newPage.ID, SlotID: uint32(slotID)}, nil
		}

		th.bufferPool.UnpinPage(currPageID, false)
		currPageID = nextID
	}
}

// GetTuple retrieves a tuple by its RID.
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

	out := make([]byte, len(data))
	copy(out, data)
	return out, nil
}

// TableIterator iterates over all tuples in the heap.
type TableIterator struct {
	tableHeap  *TableHeap
	currPageID PageID
	currSlot   int
}

// Iterator returns a new iterator starting from the first page.
func (th *TableHeap) Iterator() *TableIterator {
	return &TableIterator{
		tableHeap:  th,
		currPageID: th.firstPageID,
		currSlot:   0,
	}
}

// Next returns the next tuple, or nil when exhausted.
func (it *TableIterator) Next() ([]byte, RID, error) {
	for {
		if it.currPageID == InvalidPageID {
			return nil, RID{}, nil
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
			it.tableHeap.bufferPool.UnpinPage(it.currPageID, false)
			continue
		}

		nextID := sp.GetNextPageID()
		it.tableHeap.bufferPool.UnpinPage(it.currPageID, false)
		it.currPageID = nextID
		it.currSlot = 0
	}
}
