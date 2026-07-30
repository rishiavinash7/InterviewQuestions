import threading
from typing import Any, Hashable, Optional


class _Node:
    __slots__ = ('key', 'value', 'prev', 'next')
    def __init__(self, key: Hashable, value: Any):
        self.key = key
        self.value = value
        self.prev: Optional['_Node'] = None
        self.next: Optional['_Node'] = None


class LRUCache:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self._map = dict[Hashable, _Node] = {}
        self._head = _Node(None, None)
        self._tail = _Node(None, None)
        self._head.next = self._tail
        self._tail.prev = self._head

    def _remove(self, node: _Node)->None:
        node.prev.next = node.next
        node.next.prev = node.prev

    def _add_to_front(self, node: _Node)->None:
        node.next = self._head.next
        self._head.next.prev = node
        node.prev = self._head
        self._head.next = node

    def get(self, key: Hashable)->Any:
        node = self._map.get(key)
        if node is None:
            return KeyError(key)
        self._remove(node)
        self._add_to_front(node)
        return node.value
    
    def put(self, key: Hashable, value: Any)->None:
        existing = self._map.get(key)
        if existing is not None:
            existing.value = value
            self._remove(existing)
            self._add_to_front(existing)
            return
        if len(self._map)>=self.capacity:
            lru = self._tail.prev
            self._remove(lru)
            del self._map[lru.key]
        node = _Node(key, value)
        self._add_to_front(node)
        self._map['key'] = node

    def __len__(self)->int:
        return len(self._map)
    
    def __contains__(self, key: Hashable)->bool:
        return key in self._map
    



            


    

    