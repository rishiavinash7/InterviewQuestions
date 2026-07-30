import time
import threading
from dataclasses import dataclass, field


@dataclass
class LeakyBucketMeter:
    capacity: float
    leak_rate: float
    _level: float = field(init=False, default=0)
    _last_leak: float = field(init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)


    def __post_init__(self):
        self._last_leak = time.monotonic()

    
    def _leak(self):
        now = time.monotonic()
        elapsed = now-self._last_leak
        self._level = max(0, self._level-elapsed*self.leak_rate)
        self._last_leak = now

    
    def try_add(self, amount: float=1.0)->bool:
        with self._lock:
            self._leak()
            if self._level+amount<=self.capacity:
                self._level+=amount
                return True
            return False
        

        