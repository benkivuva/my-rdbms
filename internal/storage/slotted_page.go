package storage

import (
	"encoding/binary"
	"fmt"
)

// SlottedPage Layout (4KB):
// Header (12 bytes):
//   [0-7]:   NextPageID (int64)
//   [8-9]:   NumSlots (uint16)
//   [10-11]: FreeSpacePointer (uint16)
// Slot Array: [Offset(2), Length(2)] per slot
// Tuple Data: grows from end of page toward header

const (
	OffsetNextPageID = 0
	OffsetNumSlots   = 8
	OffsetFreeSpace  = 10
	SizeOfHeader     = 12
	SizeOfSlot       = 4
)

// SlottedPage provides tuple storage within a fixed-size page.
type SlottedPage struct {
	page *Page
}

// NewSlottedPage wraps a page for slotted access.
func NewSlottedPage(page *Page) *SlottedPage {
	sp := &SlottedPage{page: page}
	if sp.GetNumSlots() == 0 && sp.GetFreeSpacePointer() == 0 {
		sp.SetFreeSpacePointer(PageSize)
	}
	return sp
}

func (sp *SlottedPage) GetNumSlots() uint16 {
	return binary.BigEndian.Uint16(sp.page.Data[OffsetNumSlots:])
}

func (sp *SlottedPage) SetNumSlots(num uint16) {
	binary.BigEndian.PutUint16(sp.page.Data[OffsetNumSlots:], num)
}

func (sp *SlottedPage) GetFreeSpacePointer() uint16 {
	return binary.BigEndian.Uint16(sp.page.Data[OffsetFreeSpace:])
}

func (sp *SlottedPage) SetFreeSpacePointer(ptr uint16) {
	binary.BigEndian.PutUint16(sp.page.Data[OffsetFreeSpace:], ptr)
}

func (sp *SlottedPage) GetSlot(slotIdx int) (uint16, uint16) {
	offset := SizeOfHeader + slotIdx*SizeOfSlot
	sOff := binary.BigEndian.Uint16(sp.page.Data[offset:])
	sLen := binary.BigEndian.Uint16(sp.page.Data[offset+2:])
	return sOff, sLen
}

func (sp *SlottedPage) SetSlot(slotIdx int, sOff, sLen uint16) {
	offset := SizeOfHeader + slotIdx*SizeOfSlot
	binary.BigEndian.PutUint16(sp.page.Data[offset:], sOff)
	binary.BigEndian.PutUint16(sp.page.Data[offset+2:], sLen)
}

// InsertTuple adds data to the page. Returns slot ID or error if full.
func (sp *SlottedPage) InsertTuple(data []byte) (int, error) {
	needed := len(data)
	if needed > PageSize {
		return -1, fmt.Errorf("tuple too large")
	}

	numSlots := int(sp.GetNumSlots())
	freePtr := int(sp.GetFreeSpacePointer())
	usedHeader := SizeOfHeader + numSlots*SizeOfSlot
	available := freePtr - usedHeader

	if available < SizeOfSlot+needed {
		return -1, fmt.Errorf("no space")
	}

	newFreePtr := freePtr - needed
	copy(sp.page.Data[newFreePtr:freePtr], data)
	sp.SetFreeSpacePointer(uint16(newFreePtr))

	sp.SetSlot(numSlots, uint16(newFreePtr), uint16(needed))
	sp.SetNumSlots(uint16(numSlots + 1))

	return numSlots, nil
}

// GetTuple reads data from the given slot.
func (sp *SlottedPage) GetTuple(slotIdx int) []byte {
	if slotIdx >= int(sp.GetNumSlots()) {
		return nil
	}
	off, length := sp.GetSlot(slotIdx)
	if length == 0 {
		return nil
	}
	return sp.page.Data[off : off+length]
}

// DeleteTuple marks a slot as deleted by setting its length to 0.
func (sp *SlottedPage) DeleteTuple(slotIdx int) bool {
	if slotIdx >= int(sp.GetNumSlots()) {
		return false
	}
	off, _ := sp.GetSlot(slotIdx)
	sp.SetSlot(slotIdx, off, 0)
	return true
}
