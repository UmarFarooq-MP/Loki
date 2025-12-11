package orderbook

type Color uint8

const (
	red   Color = 0
	black Color = 1
)

type node struct {
	key    int64
	level  *PriceLevel
	color  Color
	left   *node
	right  *node
	parent *node
}

type RBTree struct {
	root *node
	nil  *node
	size int
}

func NewRBTree() *RBTree {
	nilNode := &node{color: black}
	return &RBTree{root: nilNode, nil: nilNode}
}

func (t *RBTree) Size() int { return t.size }

func (t *RBTree) FindLevel(price int64) *PriceLevel {
	n := t.root
	for n != t.nil {
		switch {
		case price < n.key:
			n = n.left
		case price > n.key:
			n = n.right
		default:
			return n.level
		}
	}
	return nil
}

func (t *RBTree) UpsertLevel(price int64) *PriceLevel {
	y := t.nil
	x := t.root
	for x != t.nil {
		y = x
		if price < x.key {
			x = x.left
		} else if price > x.key {
			x = x.right
		} else {
			return x.level
		}
	}
	pl := &PriceLevel{Price: price}
	z := &node{key: price, level: pl, color: red, left: t.nil, right: t.nil, parent: y}
	if y == t.nil {
		t.root = z
	} else if z.key < y.key {
		y.left = z
	} else {
		y.right = z
	}
	t.insertFixup(z)
	t.size++
	return pl
}

func (t *RBTree) DeleteLevel(price int64) bool {
	z := t.searchNode(price)
	if z == t.nil {
		return false
	}
	t.deleteNode(z)
	t.size--
	return true
}

func (t *RBTree) MinLevel() *PriceLevel {
	n := t.minNode(t.root)
	if n == t.nil {
		return nil
	}
	return n.level
}

func (t *RBTree) MaxLevel() *PriceLevel {
	n := t.maxNode(t.root)
	if n == t.nil {
		return nil
	}
	return n.level
}

func (t *RBTree) ForEachAscending(fn func(*PriceLevel) bool) {
	for n := t.minNode(t.root); n != t.nil; n = t.next(n) {
		if !fn(n.level) {
			return
		}
	}
}

func (t *RBTree) ForEachDescending(fn func(*PriceLevel) bool) {
	for n := t.maxNode(t.root); n != t.nil; n = t.prev(n) {
		if !fn(n.level) {
			return
		}
	}
}

// --- internals omitted for brevity (rotations/fixups/min/max/next/prev) ---
// use your existing tested implementation for them
