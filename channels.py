"""
PA 10: Channels -- starter (final PA of the semester).

Complete Channel below, plus the producer/consumer driver at the
bottom. See the assignment, Part B, for the full requirements.
Values passed through the channel are GCHeap handles (ints) -- see
gc_heap.py.
"""

import threading
from collections import deque
from typing import Any, Optional

from gc_heap import GCHeap

_CLOSED_SENTINEL = object()


class Channel:
    def __init__(self, capacity: int, heap: GCHeap) -> None:
        self._capacity = capacity
        self._heap = heap
        self._buffer: deque = deque()
        self._cond = threading.Condition()
        self._closed = False

    def put(self, value: Any) -> None:
        """
        Block while the buffer is full. `value` is expected to be a
        GCHeap handle -- incref it (the channel now holds a reference)
        BEFORE enqueuing. Wake a waiting get() once space is available.
        """
        with self._cond:
            while True:
                if self._closed:
                    raise RuntimeError("put() on a closed channel")
                if len(self._buffer) < self._capacity:
                    break
                self._cond.wait()


            self._heap.incref(value)
            self._buffer.append(value)
            self._cond.notify_all()

    def get(self):
        """
        Block while the buffer is empty (and not closed). Returns the
        next value, transferring the channel's reference to the
        caller (do NOT decref here -- the caller now owns that
        reference and is responsible for eventually releasing it).
        On an empty, closed channel, return the sentinel from
        `Channel.CLOSED` instead of blocking forever.
        """
        with self._cond:
            while not self._buffer and not self._closed:
                self._cond.wait()

            if self._buffer:

                value = self._buffer.popleft()
                self._cond.notify_all()
                return value

            return Channel.CLOSED

    def close(self) -> None:
        """Mark closed and wake every blocked thread."""
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    CLOSED = _CLOSED_SENTINEL


def producer_consumer_driver(heap: GCHeap, values: list, channel: Channel, num_consumers: int = 1):
    """
    Given: a driver you can use as-is (or adapt) for your own testing.
    One producer thread allocates + puts each value in `values` onto
    `channel`, then closes it. `num_consumers` consumer threads pull
    handles off, look up and record their value, then release their
    transferred reference, exiting cleanly on the CLOSED sentinel.
    Returns the list of delivered (handle, value) pairs.
    """
    delivered = []
    delivered_lock = threading.Lock()

    def produce():
        for v in values:
            handle = heap.allocate(v)
            try:
                channel.put(handle)
            finally:
                heap.decref(handle)
        channel.close()

    def consume():
        while True:
            handle = channel.get()
            if handle is Channel.CLOSED:
                return
 
            value = heap._table[handle][0]
            with delivered_lock:
                delivered.append((handle, value))
            heap.decref(handle) 

    producer_thread = threading.Thread(target=produce)
    consumer_threads = [threading.Thread(target=consume) for _ in range(num_consumers)]
    for t in [producer_thread] + consumer_threads:
        t.start()
    producer_thread.join()
    for t in consumer_threads:
        t.join(timeout=10.0)

    return delivered
