# Given a stream of user messages, batch them into groups of N or every T ms (whichever hits first) and send to a mock LLM

# You're collecting messages one at a time but want to send them in batches to save API calls. Two triggers:

# Size trigger: collected N messages → send.
# Time trigger: T milliseconds have passed since the first message in this batch → send, even if you have fewer than N.

# Whichever fires first wins. This is exactly how OpenAI's batch mode, Kafka producer batching, and most "send in bulk" systems work.


import asyncio
import random


async def mock_llm(batch):
    """Fake LLM call — takes a batch, returns responses."""
    await asyncio.sleep(0.2)  # pretend network latency
    print(f"  → LLM processed batch of {len(batch)}: {batch}")
    return [f"reply-to-{msg}" for msg in batch]


async def batcher(queue, batch_size, timeout_ms):
    timeout_s = timeout_ms / 1000
    while True:
        batch = []
        # Wait for the FIRST item without a timeout — no point starting the
        # timer on an empty batch.
        first = await queue.get()
        if first is None:  # shutdown signal
            return
        batch.append(first)
        deadline = asyncio.get_event_loop().time() + timeout_s

        # Collect more items until batch is full OR deadline hits.
        while len(batch) < batch_size:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                break  # time trigger fired
            try:
                item = await asyncio.wait_for(queue.get(), timeout=remaining)
            except asyncio.TimeoutError:
                break  # time trigger fired
            if item is None:  # shutdown mid-batch
                await mock_llm(batch)
                return
            batch.append(item)

        await mock_llm(batch)


async def producer(queue, n_messages):
    for i in range(n_messages):
        await queue.put(f"msg-{i}")
        await asyncio.sleep(random.uniform(0.02, 0.15))  # irregular arrival
    await queue.put(None)  # shutdown signal


async def main():
    queue = asyncio.Queue()
    batch_size = 5
    timeout_ms = 300

    await asyncio.gather(
        producer(queue, n_messages=20),
        batcher(queue, batch_size, timeout_ms),
    )


asyncio.run(main())