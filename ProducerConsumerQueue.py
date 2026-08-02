import asyncio
import random
from dataclasses import dataclass


@dataclass
class WorkItem:
    id: int
    payload: str


async def producer(queue: asyncio.Queue, n_items: int, producer_id: int):
    for i in range(n_items):
        item = WorkItem(id=i, payload=f"doc-{producer_id}-{i}")
        # This is where backpressure happens: put() blocks if queue is full.
        await queue.put(item)
        print(f"[P{producer_id}] produced {item.id} (queue size: {queue.qsize()})")


async def consumer(queue: asyncio.Queue, consumer_id: int):
    while True:
        item = await queue.get()
        try:
            # Simulate variable-latency work (e.g., an LLM call).
            await asyncio.sleep(random.uniform(0.1, 0.3))
            print(f"[C{consumer_id}] processed {item.payload}")
        finally:
            queue.task_done()


async def main():
    queue = asyncio.Queue(maxsize=10)  # bounded — this is the backpressure knob
    
    # Start 3 consumers.
    consumers = [
        asyncio.create_task(consumer(queue, cid))
        for cid in range(3)
    ]
    
    # Run 2 producers concurrently.
    producers = [
        asyncio.create_task(producer(queue, n_items=20, producer_id=pid))
        for pid in range(2)
    ]
    
    # Wait for all producers to finish emitting items.
    await asyncio.gather(*producers)
    
    # Wait for all queued items to be processed.
    await queue.join()
    
    # Consumers are in `while True` loops — cancel them cleanly.
    for c in consumers:
        c.cancel()
    await asyncio.gather(*consumers, return_exceptions=True)


asyncio.run(main())




# The whole idea in one sentence

# Some code makes stuff (producer), other code processes it (consumer), and a box in the middle (queue) holds stuff waiting to be processed. The box has a size limit so it doesn't fill up your memory.

# The code, section by section
# python
# import asyncio
# import random
# from dataclasses import dataclass

# Standard imports. asyncio for running things at the same time. random just to fake different processing times. dataclass to make a simple data holder without writing boilerplate.

# python
# @dataclass
# class WorkItem:
#     id: int
#     payload: str

# What it is: a container for one unit of work. Think of it as a package with a label (id) and contents (payload).

# Why: we need something to put in the queue. Could be a dict, a tuple, anything. A dataclass is just clean.

# Example: WorkItem(id=5, payload="doc-1-5") creates one package.

# python
# async def producer(queue, n_items, producer_id):
#     for i in range(n_items):
#         item = WorkItem(id=i, payload=f"doc-{producer_id}-{i}")
#         await queue.put(item)
#         print(f"[P{producer_id}] produced {item.id}")

# What it does: creates n_items packages and puts them in the queue.

# The key line — await queue.put(item):

# If the queue has space → put the item in, keep going.
# If the queue is full → stop and wait until there's space.

# That waiting is the whole point. The producer physically cannot outpace the consumer because the box is full and it's stuck at the door.

# When you'd use this: anywhere you're generating work faster than something else can handle it. Reading files to send to an API. Downloading pages to parse. Fetching rows to write to a database.

# python
# async def consumer(queue, consumer_id):
#     while True:
#         item = await queue.get()
#         try:
#             await asyncio.sleep(random.uniform(0.1, 0.3))
#             print(f"[C{consumer_id}] processed {item.payload}")
#         finally:
#             queue.task_done()

# What it does: grabs one item from the queue, processes it, repeats forever.

# while True: consumers don't know when to stop. They just keep pulling items. We'll stop them from outside later.

# await queue.get():

# If there's an item → grab it.
# If the queue is empty → stop and wait until something appears.

# await asyncio.sleep(...): pretending to do work for 100-300ms. In real code this would be your HTTP call, database write, or LLM call.

# Why asyncio.sleep and not time.sleep: time.sleep freezes everything. asyncio.sleep says "I'm waiting, let other things run." That's how multiple consumers work at the same time on one thread.

# try / finally around task_done(): even if processing crashes, we must call task_done(). If we don't, the program will hang forever at shutdown (explained below). finally guarantees it runs no matter what.

# queue.task_done(): tells the queue "I'm done with this item." Every put needs a matching task_done.

# python
# async def main():
#     queue = asyncio.Queue(maxsize=10)

# maxsize=10: the box holds at most 10 items. This one number is your entire memory-safety mechanism.

# Too small → producers wait too often, wastes time.
# Too big → uses too much memory, defeats the point.
# maxsize=0 (default) → unlimited size, dangerous.

# Rule of thumb: something like 2-3× the number of consumers is usually fine.

# python
#     consumers = [
#         asyncio.create_task(consumer(queue, cid))
#         for cid in range(3)
#     ]

# What it does: starts 3 consumers running in the background.

# asyncio.create_task(...): "start this running now, don't wait for it to finish, give me a handle to control it later." Like clicking a button that launches something and gives you a remote.

# Why we save the handles in consumers: we need them later to stop the consumers.

# python
#     producers = [
#         asyncio.create_task(producer(queue, n_items=20, producer_id=pid))
#         for pid in range(2)
#     ]

# Same thing: 2 producers running in the background, each will make 20 items. Total 40 items.

# python
#     await asyncio.gather(*producers)

# What it does: wait for both producers to finish making all their items.

# asyncio.gather(*producers): "wait for all these background tasks to complete."

# Important: after this line, producers are done, but there might still be up to 10 items in the queue waiting to be processed. We're NOT done yet.

# python
#     await queue.join()

# What it does: wait until every item that was ever put in the queue has been processed.

# How it knows: the queue keeps a counter.

# Every put → counter +1
# Every task_done → counter −1
# When counter = 0, join() unblocks.

# Now you see why task_done() in finally matters: if a consumer crashes and skips task_done(), the counter never hits zero, and queue.join() waits forever. Program hangs.

# python
#     for c in consumers:
#         c.cancel()
#     await asyncio.gather(*consumers, return_exceptions=True)

# Why: consumers are stuck in while True waiting for items that will never come. We have to force them to stop.

# c.cancel(): politely interrupt each consumer. It'll raise an error inside them, they exit their loop.

# await asyncio.gather(*consumers, return_exceptions=True): wait for them to actually finish stopping.

# return_exceptions=True: without this, the cancellation would look like a crash and get raised. This flag says "cancellation is expected, don't complain."

# python
# asyncio.run(main())

# The entry point that runs everything. You use this once, at the very top level.

# What actually happens when you run it
# Queue starts empty. 3 consumers wait at get(). 2 producers start putting items.
# Queue fills to 10. Producers stall at put(). Consumers pull, process for 100-300ms, pull again.
# Steady state: queue stays near 10, producers only put when consumers pull. Producer speed = consumer speed. That's backpressure.
# Producers finish all 40 items. gather(*producers) unblocks.
# Consumers keep draining the remaining items. queue.join() waits.
# Last item processed, counter hits 0, join() unblocks.
# We cancel consumers, wait for them to stop, program ends.
# When to use which piece
# You need to...	Use
# Cap memory in a pipeline	asyncio.Queue(maxsize=N)
# Add work to the pipeline	await queue.put(item)
# Pull work from the pipeline	await queue.get()
# Mark an item as fully processed	queue.task_done() (in finally)
# Wait until all work is done	await queue.join()
# Start background work	asyncio.create_task(...)
# Wait for background tasks to finish	await asyncio.gather(*tasks)
# Stop background tasks that never end on their own	task.cancel() then gather(..., return_exceptions=True)

# That's the whole pattern. Copy it, change the producer and consumer bodies to whatever your actual work is, and you have a memory-safe pipeline.





# this is too much information to take in at once with too much complications the language is not simple you use complex terms like coroutine and all - just give me the code and explain what it does why we use what we use with simple examples so i can understand this line of code behaves this way in this condition and i should relate where to use that particular line of code next - don't include unnecessary information and make it long