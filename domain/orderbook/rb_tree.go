package orderbook

type rbNode struct {
	key    int64
	level  *PriceLevel
	left   *rbNode
	right  *rbNode
	parent *rbNode
}

type RBTree struct {
	root *rbNode
	nil  *rbNode
}

func NewRBTree() *RBTree {
	nilNode := &rbNode{}
	return &RBTree{
		root: nilNode,
		nil:  nilNode,
	}
}

// ---- public API ----

func (t *RBTree) GetOrCreate(price int64) *PriceLevel {
	n := t.find(price)
	if n != t.nil {
		return n.level
	}

	lvl := &PriceLevel{Price: price}
	t.insert(price, lvl)
	return lvl
}

func (t *RBTree) Find(price int64) *PriceLevel {
	n := t.find(price)
	if n == t.nil {
		return nil
	}
	return n.level
}

func (t *RBTree) BestMin() *PriceLevel {
	n := t.min(t.root)
	if n == t.nil {
		return nil
	}
	return n.level
}

func (t *RBTree) BestMax() *PriceLevel {
	n := t.max(t.root)
	if n == t.nil {
		return nil
	}
	return n.level
}

// ---- walkers ----

func (t *RBTree) walkAsc(fn func(*PriceLevel)) {
	for n := t.min(t.root); n != t.nil; n = t.next(n) {
		fn(n.level)
	}
}

func (t *RBTree) walkDesc(fn func(*PriceLevel)) {
	for n := t.max(t.root); n != t.nil; n = t.prev(n) {
		fn(n.level)
	}
}

// ---- internal helpers ----

func (t *RBTree) find(price int64) *rbNode {
	n := t.root
	for n != t.nil {
		if price < n.key {
			n = n.left
		} else if price > n.key {
			n = n.right
		} else {
			return n
		}
	}
	return t.nil
}

func (t *RBTree) min(n *rbNode) *rbNode {
	for n != t.nil && n.left != t.nil {
		n = n.left
	}
	return n
}

func (t *RBTree) max(n *rbNode) *rbNode {
	for n != t.nil && n.right != t.nil {
		n = n.right
	}
	return n
}

func (t *RBTree) next(n *rbNode) *rbNode {
	if n.right != t.nil {
		return t.min(n.right)
	}
	p := n.parent
	for p != t.nil && n == p.right {
		n = p
		p = p.parent
	}
	return p
}

func (t *RBTree) prev(n *rbNode) *rbNode {
	if n.left != t.nil {
		return t.max(n.left)
	}
	p := n.parent
	for p != t.nil && n == p.left {
		n = p
		p = p.parent
	}
	return p
}

// NOTE: insert balancing logic should be pasted here from your
// existing RB-tree implementation unchanged.
func (t *RBTree) insert(price int64, lvl *PriceLevel) {
	// reuse your existing correct RB insert code here
}
