import time 
import threading
from dataclasses import dataclass, field
import asyncio


@dataclass
class TokenBucket:
    capacity: float
    refill_rate: float
    _tokens: float = field(init=False)
    _last_refill: float = field(init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)


    def __post_init__(self):
        self._tokens = self.capacity
        self._last_refill = time.monotonic()

    
    def _refill(self):
        now = time.monotonic()
        elapsed = now-self._last_refill
        self._tokens = min(self.capacity, self._tokens+elapsed*self.refill_rate)
        self._last_refill = now

    
    def try_consume(self, tokens:float=1.0) -> bool:
        with self._lock:
            self._refill()
            if self._tokens>=tokens:
                self._tokens-=tokens
                return True
            return False
        
    def consume(self, tokens:float=1.0, timeout:float|None=None)->bool:
        deadline = None if timeout is None else time.monotonic()+timeout
        while True:
            with self._lock:
                self._refill()
                if self._tokens>=tokens:
                    self._tokens-=tokens
                    return True
                deficit = tokens-self._tokens
                wait = deficit/self.refill_rate

            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining<=0:
                    return False
                wait = min(wait, remaining)
            time.sleep(wait)

    


class AsyncTokenBucket:
    def __init__(self, capacity: float, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._tokens = capacity
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()


    def _refill(self):
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens+elapsed*self.refill_rate)
        self._last_refill = now

    
    async def acquire(self, tokens:float=1.0):
        while True:
            async with self._lock:
                self._refill()
                if self._tokens>=tokens:
                    self._tokens-=tokens
                    return 
                wait = (tokens-self._tokens)/self.refill_rate
            await asyncio.sleep(wait)


if __name__=="__main__":
    bucket = TokenBucket(capacity=20, refill_rate=10)
    if bucket.try_consume():
        print("request allowed")
    else:
        print("rate limited")

    if bucket.consume(tokens=1, timeout=5):
        print("got token within 5s")

