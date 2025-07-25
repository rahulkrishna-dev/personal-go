package linkedlist

//
//import (
//	"fmt"
//)
//
//func (list *LRUCache[K]) AddNodeBeforeTail(val K) {
//	prev := list.LinkedList.Tail.Prev
//	node := &Node[K]{
//		Data: val,
//		Prev: prev,
//		Next: list.Tail,
//	}
//	prev.Next = node
//	list.Positions[val] = node
//}
//
//func (list *LRUCache[K]) RemoveNode(val K) error {
//	pos, ok := list.Positions[val]
//	if !ok {
//		return fmt.Errorf("node doesn't exist")
//	}
//	prev := pos.Prev
//	next := pos.Next
//	prev.Next = next
//	next.Prev = prev
//	delete(list.Positions, val)
//	return nil
//}
//
//func (list *LRUCache[K]) get(val K) error {
//	pos, ok := list.Positions[val]
//	if !ok {
//		return fmt.Errorf("node doesn't exist")
//	}
//	x
//}
