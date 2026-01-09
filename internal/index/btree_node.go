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

// BTreeNode Layout:
// Header (24 bytes):
//   [0-3]:   NodeType (uint32)
//   [4-7]:   NumKeys (uint32)
//   [8-15]:  ParentPageID (int64)
//   [16-23]: NextPageID (int64, for leaf linking)
//
// Body:
//   Internal: [Key(8), ChildPageID(8)] pairs (16 bytes each)
//   Leaf:     [Key(8), RID(12)] pairs (20 bytes each)

const HeaderSize = 24

// BTreeNode wraps a page to provide B-Tree node operations.
type BTreeNode struct {
	data []byte
}

// NewBTreeNode creates a B-Tree node view over a page.
func NewBTreeNode(page *storage.Page) *BTreeNode {
	return &BTreeNode{data: page.GetData()}
}

// Init initializes the node with the given type.
func (n *BTreeNode) Init(nodeType uint32) {
	n.SetNodeType(nodeType)
	n.SetNumKeys(0)
	n.SetParentPageID(-1)
	n.SetNextPageID(-1)
}

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
	return storage.PageID(int64(binary.BigEndian.Uint64(n.data[8:16])))
}

func (n *BTreeNode) SetParentPageID(pid storage.PageID) {
	binary.BigEndian.PutUint64(n.data[8:16], uint64(pid))
}

func (n *BTreeNode) GetNextPageID() storage.PageID {
	return storage.PageID(int64(binary.BigEndian.Uint64(n.data[16:24])))
}

func (n *BTreeNode) SetNextPageID(pid storage.PageID) {
	binary.BigEndian.PutUint64(n.data[16:24], uint64(pid))
}

func (n *BTreeNode) IsLeaf() bool {
	return n.GetNodeType() == NodeTypeLeaf
}

func (n *BTreeNode) GetKey(i int) int64 {
	offset := n.getKeyOffset(i)
	return int64(binary.BigEndian.Uint64(n.data[offset : offset+8]))
}

func (n *BTreeNode) SetKey(i int, key int64) {
	offset := n.getKeyOffset(i)
	binary.BigEndian.PutUint64(n.data[offset:offset+8], uint64(key))
}

func (n *BTreeNode) GetValuePageID(i int) storage.PageID {
	offset := n.getValueOffset(i)
	return storage.PageID(int64(binary.BigEndian.Uint64(n.data[offset : offset+8])))
}

func (n *BTreeNode) SetValuePageID(i int, val storage.PageID) {
	offset := n.getValueOffset(i)
	binary.BigEndian.PutUint64(n.data[offset:offset+8], uint64(val))
}

func (n *BTreeNode) GetValueRID(i int) storage.RID {
	offset := n.getValueOffset(i)
	pid := storage.PageID(int64(binary.BigEndian.Uint64(n.data[offset : offset+8])))
	sid := binary.BigEndian.Uint32(n.data[offset+8 : offset+12])
	return storage.RID{PageID: pid, SlotID: sid}
}

func (n *BTreeNode) SetValueRID(i int, val storage.RID) {
	offset := n.getValueOffset(i)
	binary.BigEndian.PutUint64(n.data[offset:offset+8], uint64(val.PageID))
	binary.BigEndian.PutUint32(n.data[offset+8:offset+12], val.SlotID)
}

func (n *BTreeNode) getKeyOffset(i int) int {
	pairSize := 16
	if n.IsLeaf() {
		pairSize = 20
	}
	return HeaderSize + i*pairSize
}

func (n *BTreeNode) getValueOffset(i int) int {
	return n.getKeyOffset(i) + 8
}

// MaxCapacity returns the maximum number of key-value pairs.
func (n *BTreeNode) MaxCapacity() int {
	pairSize := 16
	if n.IsLeaf() {
		pairSize = 20
	}
	return (storage.PageSize - HeaderSize) / pairSize
}

// InsertLeaf inserts a key/RID pair into a leaf node.
func (n *BTreeNode) InsertLeaf(key int64, rid storage.RID) bool {
	num := int(n.GetNumKeys())
	if num >= n.MaxCapacity() {
		return false
	}

	idx := sort.Search(num, func(i int) bool {
		return n.GetKey(i) >= key
	})

	pairSize := 20
	src := HeaderSize + idx*pairSize
	dest := src + pairSize
	count := (num - idx) * pairSize
	copy(n.data[dest:dest+count], n.data[src:src+count])

	n.SetKey(idx, key)
	n.SetValueRID(idx, rid)
	n.SetNumKeys(uint32(num + 1))
	return true
}

// InsertInternal inserts a key/child pair into an internal node.
func (n *BTreeNode) InsertInternal(key int64, val storage.PageID) bool {
	num := int(n.GetNumKeys())
	if num >= n.MaxCapacity() {
		return false
	}

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

// SplitLeaf moves half of the items to the recipient node.
func (n *BTreeNode) SplitLeaf(recipient *BTreeNode, recipientPageID storage.PageID) int64 {
	total := int(n.GetNumKeys())
	splitIdx := total / 2
	moveCount := total - splitIdx

	recipient.Init(NodeTypeLeaf)

	pairSize := 20
	startOffset := n.getKeyOffset(splitIdx)
	dataLen := moveCount * pairSize

	copy(recipient.data[HeaderSize:HeaderSize+dataLen], n.data[startOffset:startOffset+dataLen])
	recipient.SetNumKeys(uint32(moveCount))

	n.SetNumKeys(uint32(splitIdx))

	recipient.SetNextPageID(n.GetNextPageID())
	n.SetNextPageID(recipientPageID)

	return recipient.GetKey(0)
}
