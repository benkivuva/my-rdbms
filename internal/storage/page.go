package storage

import "encoding/binary"

const PageSize = 4096

type PageID int

// Page represents a fixed-size block of data.
type Page struct {
	ID       PageID
	PinCount int32
	IsDirty  bool
	Data     [PageSize]byte
}

// NewPage creates a new empty page with the given ID.
func NewPage(id PageID) *Page {
	return &Page{
		ID: id,
	}
}

// GetData returns the byte slice of the page data.
func (p *Page) GetData() []byte {
	return p.Data[:]
}

// Copy copies data into the page.
func (p *Page) Copy(data []byte) {
	copy(p.Data[:], data)
}

// Clear resets the page data.
func (p *Page) Clear() {
	for i := range p.Data {
		p.Data[i] = 0
	}
}

// SetInt writes an int32 at the given offset.
func (p *Page) SetInt(offset int, val int32) {
	binary.BigEndian.PutUint32(p.Data[offset:], uint32(val))
}

// GetInt reads an int32 from the given offset.
func (p *Page) GetInt(offset int) int32 {
	return int32(binary.BigEndian.Uint32(p.Data[offset:]))
}
