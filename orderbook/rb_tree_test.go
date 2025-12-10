package orderbook

import "testing"

func TestRBTreeInsertFindDelete(t *testing.T) {
	tree := NewRBTree()
	pl1 := tree.UpsertLevel(100)
	if pl1 == nil {
		t.Fatal("UpsertLevel failed")
	}
	if pl2 := tree.FindLevel(100); pl2 != pl1 {
		t.Error("FindLevel did not return same PriceLevel")
	}

	tree.UpsertLevel(200)
	if tree.MinLevel().Price != 100 {
		t.Error("expected min=100")
	}
	if tree.MaxLevel().Price != 200 {
		t.Error("expected max=200")
	}

	if !tree.DeleteLevel(100) {
		t.Error("DeleteLevel failed")
	}
	if tree.FindLevel(100) != nil {
		t.Error("expected level 100 to be gone")
	}
}

// --- Edge Cases ---

func TestDeleteNonExistentLevel(t *testing.T) {
	tree := NewRBTree()
	if tree.DeleteLevel(123) {
		t.Error("expected false when deleting non-existent level")
	}
}

func TestEmptyTreeMinMax(t *testing.T) {
	tree := NewRBTree()
	if tree.MinLevel() != nil || tree.MaxLevel() != nil {
		t.Error("expected nil for min/max on empty tree")
	}
}

func TestUpsertDuplicateLevel(t *testing.T) {
	tree := NewRBTree()
	pl1 := tree.UpsertLevel(150)
	pl2 := tree.UpsertLevel(150)
	if pl1 != pl2 {
		t.Error("Upsert should return the same node for duplicate level")
	}
}
