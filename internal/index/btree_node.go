package index

import (
	"encoding/binary"
	"sort"

	"github.com/benkivuva/my-rdbms/internal/storage"
)

const (
	NodeTypeInternal = 1
	NodeTypeLeaf     = 2
)

// BTree Node Layout:
// Header:
// [0-3]: PageType (uint32) - for alignment
// [4-7]: NumKeys (uint32)
// [8-15]: ParentPageID (int64/PageID)
// [16-23]: NextPageID (int64/PageID) - Only used for Leaf, but we can reserve it.
// Total Header Size = 24 bytes.

// Internal Node Body:
// Array of [Key(8), ChildPageID(8)]
// We treat the first child pointer as a special case or include it in the array.
// Strategy: Keys[i] < Children[i]. Value(PageID). 
// Simplification: We store (Key, Value) pairs. 
// For Internal Node: Value is PageID (8 bytes). Key is int64 (8 bytes). Pair=16 bytes.
// For Leaf Node: Value is RID (PageID 8 + SlotID 4 = 12 bytes). Key is int64 (8 bytes). Pair=20 bytes.

const (
	HeaderSize = 24
)

// Helper Wrapper (View) over a Page
type BTreeNode struct {
	data []byte
}

func NewBTreeNode(page *storage.Page) *BTreeNode {
	return &BTreeNode{data: page.GetData()}
}

func (n *BTreeNode) Init(nodeType uint32) {
	n.SetNodeType(nodeType)
	n.SetNumKeys(0)
	n.SetParentPageID(-1)
	n.SetNextPageID(-1)
}

// --- Header Getters/Setters ---

func (n *BTreeNode) GetNodeType() uint32 {
	return binary.BigEndian.Uint32(n.data[0:4])
}
func (n *BTreeNode) SetNodeType(t uint32) {
	binary.BigEndian.PutUint32(n.data[0:4], t)
}

func (n *BTreeNode) GetNumKeys() uint32 {
	return binary.BigEndian.Uint32(n.data[4:8])
}
func (n *BTreeNode) SetNumKeys(num uint32) {
	binary.BigEndian.PutUint32(n.data[4:8], num)
}

func (n *BTreeNode) GetParentPageID() storage.PageID {
	return storage.PageID(binary.BigEndian.Uint64(n.data[8:16]))
}
func (n *BTreeNode) SetParentPageID(pid storage.PageID) {
	binary.BigEndian.PutUint64(n.data[8:16], uint64(pid))
}

func (n *BTreeNode) GetNextPageID() storage.PageID {
	return storage.PageID(binary.BigEndian.Uint64(n.data[16:24]))
}
func (n *BTreeNode) SetNextPageID(pid storage.PageID) {
	binary.BigEndian.PutUint64(n.data[16:24], uint64(pid))
}

// --- Body Operations ---

func (n *BTreeNode) IsLeaf() bool {
	return n.GetNodeType() == NodeTypeLeaf
}

// GetKey returns the key at index i
func (n *BTreeNode) GetKey(i int) int64 {
	offset := n.getKeyOffset(i)
	return int64(binary.BigEndian.Uint64(n.data[offset : offset+8]))
}

func (n *BTreeNode) SetKey(i int, key int64) {
	offset := n.getKeyOffset(i)
	binary.BigEndian.PutUint64(n.data[offset:offset+8], uint64(key))
}

// GetValuePageID returns the PageID value at index i (for Internal Nodes)
func (n *BTreeNode) GetValuePageID(i int) storage.PageID {
	offset := n.getValueOffset(i)
	return storage.PageID(binary.BigEndian.Uint64(n.data[offset : offset+8]))
}

func (n *BTreeNode) SetValuePageID(i int, val storage.PageID) {
	offset := n.getValueOffset(i)
	binary.BigEndian.PutUint64(n.data[offset:offset+8], uint64(val))
}

// GetValueRID returns the RID value at index i (for Leaf Nodes)
func (n *BTreeNode) GetValueRID(i int) storage.RID {
	offset := n.getValueOffset(i)
	// RID is PageID(8) + SlotID(4)
	pid := storage.PageID(binary.BigEndian.Uint64(n.data[offset : offset+8]))
	sid := binary.BigEndian.Uint32(n.data[offset+8 : offset+12])
	return storage.RID{PageID: pid, SlotID: sid}
}

func (n *BTreeNode) SetValueRID(i int, val storage.RID) {
	offset := n.getValueOffset(i)
	binary.BigEndian.PutUint64(n.data[offset:offset+8], uint64(val.PageID))
	binary.BigEndian.PutUint32(n.data[offset+8:offset+12], val.SlotID)
}

// Helpers for offsets
func (n *BTreeNode) getKeyOffset(i int) int {
	// For simplicity, we interleave Key/Value: [K0, V0, K1, V1...]
	// Internal: K(8) + V(8) = 16 bytes
	// Leaf: K(8) + V(12) = 20 bytes
	pairSize := 16
	if n.IsLeaf() {
		pairSize = 20
	}
	return HeaderSize + i*pairSize
}

func (n *BTreeNode) getValueOffset(i int) int {
	return n.getKeyOffset(i) + 8 // Value comes after 8-byte Key
}

// MaxCapacity estimates how many items fit.
func (n *BTreeNode) MaxCapacity() int {
	pairSize := 16
	if n.IsLeaf() {
		pairSize = 20
	}
	// Available: PageSize - HeaderSize
	return (storage.PageSize - HeaderSize) / pairSize
}

// InsertLeaf inserts a Key/RID pair into a leaf node.
// Returns true if inserted, false if full.
// Assumes node is Leaf.
func (n *BTreeNode) InsertLeaf(key int64, rid storage.RID) bool {
	num := int(n.GetNumKeys())
	if num >= n.MaxCapacity() {
		return false
	}

	// Find insert position (sorted)
	// We can use binary search or linear for simplicity. 
    // Linear scan is fine for small N (~200).
	idx := sort.Search(num, func(i int) bool {
		return n.GetKey(i) >= key
	})

	// Shift elements right
	pairSize := 20
	src := HeaderSize + idx*pairSize
	dest := src + pairSize
	count := (num - idx) * pairSize
	
    // Use copy for overlapping safety
	copy(n.data[dest:dest+count], n.data[src:src+count])

	n.SetKey(idx, key)
	n.SetValueRID(idx, rid)
	n.SetNumKeys(uint32(num + 1))
	return true
}

// InsertInternal inserts a Key/PageID pair into an internal node.
func (n *BTreeNode) InsertInternal(key int64, val storage.PageID) bool {
	num := int(n.GetNumKeys())
	if num >= n.MaxCapacity() {
		return false
	}
    
    // For internal nodes, we typically insert (Key, Child). 
    // Usually Internal nodes have N keys and N+1 children.
    // For this simple implementation, let's treat it as pairs, 
    // and maybe the 0th pointer is special or we just use (Key >= X goes to Child X).
    
    // Simplified Model: List of (Key, Child). 
    // If Key < K0 -> go to Child0? No, usually:
    // P0 K1 P1 K2 P2 ...
    
    // Let's adopt a simple "Child K is for values >= Key K" approach? 
    // Or standard: Keys separate children.
    // Let's stick to simple pairs for now and assume the first key is the lower bound for that child.
    
    idx := sort.Search(num, func(i int) bool {
		return n.GetKey(i) >= key
	})

	pairSize := 16
	src := HeaderSize + idx*pairSize
	dest := src + pairSize
	count := (num - idx) * pairSize
	copy(n.data[dest:dest+count], n.data[src:src+count])

	n.SetKey(idx, key)
	n.SetValuePageID(idx, val)
	n.SetNumKeys(uint32(num + 1))
    return true
}

// SplitLeaf moves half of the items to `recipient`.
// Returns the separation key (the first key of the new page).
func (n *BTreeNode) SplitLeaf(recipient *BTreeNode, recipientPageID storage.PageID) int64 {
	// Move right half to recipient
	total := int(n.GetNumKeys())
	splitIdx := total / 2
	moveCount := total - splitIdx
    
    recipient.Init(NodeTypeLeaf)
    
    // Copy data
    pairSize := 20
    startOffset := n.getKeyOffset(splitIdx)
    dataLen := moveCount * pairSize
    
    // Copy into recipient starting at HeaderSize
    copy(recipient.data[HeaderSize:HeaderSize+dataLen], n.data[startOffset:startOffset+dataLen])
	recipient.SetNumKeys(uint32(moveCount))
    
    n.SetNumKeys(uint32(splitIdx))
    
    // Link leaf nodes
    recipient.SetNextPageID(n.GetNextPageID())
    n.SetNextPageID(recipientPageID)
    
    return recipient.GetKey(0)
}
