import time
import threading
from dataclasses import dataclass, field
from collections import deque


@dataclass
class SlidingWindowLog:
    limit: int
    window_seconds: float
    _timestamps: deque = field(init=False, default_factory=deque)
    _lock: threading.Lock = field(init=False, default_factory=threading.Lock)


    def try_acquire(self)->bool:
        with self._lock:
            now = time.time()
            cutoff = now-self.window_seconds

            while self._timestamps and self._timestamps[0]<=cutoff:
                self._timestamps.popleft()

            if len(self._timestamps)<self.limit:
                self._timestamps.append(now)
                return True
            return False
        

            