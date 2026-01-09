package index

import (
	"fmt"

	"github.com/benkivuva/my-rdbms/internal/storage"
)

// BTreeIndex manages the B-tree structure for primary key lookups.
type BTreeIndex struct {
	bufferPool *storage.BufferPool
	rootPageID storage.PageID
}

// NewBTreeIndex creates a new B-Tree index.
// Pass InvalidPageID to allocate a new root.
func NewBTreeIndex(bp *storage.BufferPool, rootID storage.PageID) (*BTreeIndex, error) {
	bt := &BTreeIndex{
		bufferPool: bp,
		rootPageID: rootID,
	}

	if bt.rootPageID == storage.InvalidPageID {
		root, err := bt.bufferPool.NewPage()
		if err != nil {
			return nil, err
		}
		defer bt.bufferPool.UnpinPage(root.ID, true)

		node := NewBTreeNode(root)
		node.Init(NodeTypeLeaf)
		bt.rootPageID = root.ID
	}
	return bt, nil
}

// Search looks up the RID for the given key.
func (bt *BTreeIndex) Search(key int64) (storage.RID, error) {
	if bt.rootPageID == storage.InvalidPageID {
		return storage.RID{}, fmt.Errorf("empty tree")
	}

	currPageID := bt.rootPageID

	for {
		page, err := bt.bufferPool.FetchPage(currPageID)
		if err != nil {
			return storage.RID{}, err
		}
		node := NewBTreeNode(page)

		if node.IsLeaf() {
			count := int(node.GetNumKeys())
			for i := 0; i < count; i++ {
				if node.GetKey(i) == key {
					rid := node.GetValueRID(i)
					bt.bufferPool.UnpinPage(currPageID, false)
					return rid, nil
				}
			}
			bt.bufferPool.UnpinPage(currPageID, false)
			return storage.RID{}, fmt.Errorf("key %d not found", key)
		}

		// Internal node: find the appropriate child
		count := int(node.GetNumKeys())
		childPageID := storage.PageID(-1)
		for i := count - 1; i >= 0; i-- {
			if key >= node.GetKey(i) {
				childPageID = node.GetValuePageID(i)
				break
			}
		}

		if childPageID == -1 && count > 0 {
			childPageID = node.GetValuePageID(0)
		}
		if childPageID == -1 {
			bt.bufferPool.UnpinPage(currPageID, false)
			return storage.RID{}, fmt.Errorf("empty internal node")
		}

		bt.bufferPool.UnpinPage(currPageID, false)
		currPageID = childPageID
	}
}

// Insert inserts a key/RID pair into the index.
func (bt *BTreeIndex) Insert(key int64, rid storage.RID) error {
	path := make([]storage.PageID, 0)
	currPageID := bt.rootPageID

	var leafPage *storage.Page
	var leafNode *BTreeNode

	// Traverse to leaf
	for {
		path = append(path, currPageID)
		page, err := bt.bufferPool.FetchPage(currPageID)
		if err != nil {
			return err
		}
		node := NewBTreeNode(page)

		if node.IsLeaf() {
			leafPage = page
			leafNode = node
			break
		}

		count := int(node.GetNumKeys())
		childID := storage.PageID(-1)
		if count > 0 {
			childID = node.GetValuePageID(0)
			for i := count - 1; i >= 0; i-- {
				if key >= node.GetKey(i) {
					childID = node.GetValuePageID(i)
					break
				}
			}
		}
		bt.bufferPool.UnpinPage(currPageID, false)
		currPageID = childID
	}

	// Insert into leaf
	if leafNode.InsertLeaf(key, rid) {
		bt.bufferPool.UnpinPage(leafPage.ID, true)
		return nil
	}

	// Split leaf
	newPage, err := bt.bufferPool.NewPage()
	if err != nil {
		bt.bufferPool.UnpinPage(leafPage.ID, false)
		return err
	}
	newNode := NewBTreeNode(newPage)

	splitKey := leafNode.SplitLeaf(newNode, newPage.ID)

	if key >= splitKey {
		newNode.InsertLeaf(key, rid)
	} else {
		leafNode.InsertLeaf(key, rid)
	}

	bt.bufferPool.UnpinPage(leafPage.ID, true)
	bt.bufferPool.UnpinPage(newPage.ID, true)

	return bt.insertIntoParent(path, splitKey, newPage.ID)
}

func (bt *BTreeIndex) insertIntoParent(path []storage.PageID, key int64, childPageID storage.PageID) error {
	if len(path) == 1 {
		// Root split: create new root
		oldRootID := path[0]

		newRootPage, err := bt.bufferPool.NewPage()
		if err != nil {
			return err
		}
		newRoot := NewBTreeNode(newRootPage)
		newRoot.Init(NodeTypeInternal)

		minKey := int64(-1 << 63)
		newRoot.InsertInternal(minKey, oldRootID)
		newRoot.InsertInternal(key, childPageID)

		bt.rootPageID = newRootPage.ID
		bt.bufferPool.UnpinPage(newRootPage.ID, true)
		return nil
	}

	parentID := path[len(path)-2]

	parentPage, err := bt.bufferPool.FetchPage(parentID)
	if err != nil {
		return err
	}
	parentNode := NewBTreeNode(parentPage)

	if parentNode.InsertInternal(key, childPageID) {
		bt.bufferPool.UnpinPage(parentID, true)
		return nil
	}

	return fmt.Errorf("splitting internal nodes not implemented")
}
