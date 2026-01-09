package storage

// RID represents a Record ID, which points to a specific slot on a specific page.
type RID struct {
	PageID PageID
	SlotID uint32
}
