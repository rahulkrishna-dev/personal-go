package linkedlist

type Node[T any] struct {
	Data T
	Next *Node[T]
	Prev *Node[T]
}

type LinkedList[T any] struct {
	Head *Node[T]
	Tail *Node[T]
}

type LRUCache[K comparable] struct {
	LinkedList[K]
	Capacity  int
	Positions genericLookupTable[K, *Node[K]]
}

type genericLookupTable[K comparable, V any] map[K]V
