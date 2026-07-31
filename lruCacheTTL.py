import time
import threading
from typing import Any, Hashable, Optional


class _TTLNode:
    def __init__(self, key: Hashable, value: Any, expires_at: float):
        self.key = key
        self.value = value
        self.expires_at = expires_at
        self.prev: Optional['_TTLNode'] = None
        self.next: Optional['_TTLNode'] = None



class TTLCache:
    def __init__(self, capacity: int, default_ttl: Optional[float]=None):
        self.capacity = capacity
        self.default_ttl = default_ttl
        self._map: dict[Hashable, _TTLNode] = {}
        self._head = _TTLNode(None, None, 0)
        self._tail = _TTLNode(None, None, 0)
        self._head.next = self._tail
        self._tail.prev = self._head
        self._lock = threading.RLock()

    
    def _remove(self, node: _TTLNode)->None:
        node.prev.next = node.next
        node.next.prev = node.prev

    def _add_to_front(self, node: _TTLNode)->None:
        node.prev = self._head
        node.next = self._head.next
        self._head.next.prev = node
        self._head.next = node

    def _is_expired(self, node: _TTLNode, now: float)->bool:
        return node.expires_at<=now

    def _evict_node(self, node: _TTLNode)->None:
        self._remove(node)
        del self._map[node.key]

    def get(self, key:Hashable)->Any:
        with self._lock:
            node = self._map.get(key)
            if node is None:
                raise KeyError(key)
            if self._is_expired(node, time.monotonic()):
                self._evict_node(node)
                raise KeyError(key)
            self._remove(node)
            self._add_to_front(node)
            return node.value
        
    def put(self, key: Hashable, value: Any, ttl: Optional[float]=None)->None:
        with self._lock:
            effective_ttl = ttl if ttl is not None else self.default_ttl
            expires_at = time.monotonic()+effective_ttl if effective_ttl is not None else float('inf')

            existing = self._map.get(key)
            if existing is not None:
                existing.value = value
                existing.expires_at = expires_at
                self._remove(existing)
                self._add_to_front(existing)
                return
            
            if len(self._map)>=self.capacity:
                now = time.monotonic()
                candidate = self._tail.prev
                if candidate is not self._head and self._is_expired(candidate, now):
                    self._evict_node(candidate)
                else:
                    self._evict_node(self._tail.prev)

            node = _TTLNode(key, value, expires_at)
            self._map[key] = node
            self._add_to_front(node)

    def __len__(self):
        with self._lock:
            return len(self._map)
        
    def __contains__(self, key: Hashable)->bool:
        with self._lock:
            node = self._map.get(key)
            if node is None:
                return False
            if self._is_expired(node, time.monotonic()):
                self._evict_node(node)
                return False
            return True
