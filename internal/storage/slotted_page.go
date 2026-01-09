package storage

import (
	"encoding/binary"
	"fmt"
)

// Slotted Page Layout:
// Slotted Page Layout:
// Header:
//  0-8: NextPageID (PageID/int64) - 8 bytes
//  8-10: NumSlots (uint16)
//  10-12: FreeSpacePointer (uint16)
//  12+: Slot Array [Offset(2), Length(2)]

const (
	OffsetNextPageID  = 0
    OffsetNumSlots    = 8
    OffsetFreeSpace   = 10
    SizeOfHeader      = 12
	SizeOfSlot        = 4
)

type SlottedPage struct {
	page *Page
}

func NewSlottedPage(page *Page) *SlottedPage {
    sp := &SlottedPage{page: page}
    // If new page (all zeros), initialize header
    if sp.GetNumSlots() == 0 && sp.GetFreeSpacePointer() == 0 {
        sp.SetFreeSpacePointer(PageSize) // Start at end
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

// GetSlot returns struct {offset, length}
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

// InsertTuple adds data to the page. Returns slotID or error if full.
func (sp *SlottedPage) InsertTuple(data []byte) (int, error) {
    needed := len(data)
    if needed > PageSize {
        return -1, fmt.Errorf("tuple too large")
    }
    
    numSlots := int(sp.GetNumSlots())
    freePtr := int(sp.GetFreeSpacePointer())
    // Calculate space needed for new slot entry
    // Free space is between (Header + NumSlots*SizeSlot) and FreePtr
    usedHeader := SizeOfHeader + numSlots*SizeOfSlot
    available := freePtr - usedHeader
    
    // We need SizeOfSlot + needed
    if available < SizeOfSlot + needed {
        return -1, fmt.Errorf("no space")
    }
    
    // Write data
    newFreePtr := freePtr - needed
    copy(sp.page.Data[newFreePtr:freePtr], data)
    sp.SetFreeSpacePointer(uint16(newFreePtr))
    
    // Add slot
    sp.SetSlot(numSlots, uint16(newFreePtr), uint16(needed))
    sp.SetNumSlots(uint16(numSlots + 1))
    
    return numSlots, nil
}

// GetTuple reads data from slot
func (sp *SlottedPage) GetTuple(slotIdx int) []byte {
    if slotIdx >= int(sp.GetNumSlots()) {
        return nil
    }
    off, length := sp.GetSlot(slotIdx)
    if length == 0 {
        return nil // Deleted or empty
    }
    return sp.page.Data[off : off+length]
}
