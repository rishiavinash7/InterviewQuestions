import time
import threading
from dataclasses import dataclass, field


@dataclass
class FixedWindowCounter:
    limit: int
    window_seconds: int
    _count: int = field(init=False, default=0)
    _window_start: float = field(init=False, default=0.0)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    
    def _current_window_start(self)->float:
        now = time.time()
        return now - (now%self.window_seconds)
    

    def try_acquire(self)->bool:
        with self._lock:
            window_start = self._current_window_start()
            if window_start!=self._window_start:
                self._window_start=window_start
                self._count=0
            if self._count<self.limit:
                self._count+=1
                return True
            return False
        
