"""
Test Threading

These tests use multiple threads to test blocking between them, but setting
up the synchronization semaphores between threads is too much work for very
little benefit, so this is simply instructional.
"""

import time
import unittest
import logging

from typing import Any
from threading import Thread, Event

import pytest
import redis
from multilock import Multilock, CannotObtainLock

# logging
_log = logging.getLogger(__name__)

# globals
r: redis.Redis


def setup_module() -> None:
    _log.info("setup_module")
    global r

    r = redis.from_url("redis://localhost/")
    logging.debug("    - connection: %r", r)


def teardown_module() -> None:
    _log.info("teardown_module")
    global r
    r.close()


class ExclusiveThread(Thread):
    """
    This thread acquires an exclusive lock, waits for a synchronization signal,
    then releases it.
    """
    lock: Multilock

    def __init__(self, name: str, event: Event) -> None:
        super().__init__()
        _log.info("(%r) __init__", self)

        self.lock = Multilock(r, name)
        self.event = event

    def run(self):
        _log.info("(%r) run", self)

        # acquire
        self.lock.acquire_exclusive(1.0)
        _log.info("(%r) acquired", self)

        # wait for synchronization
        self.event.wait()

        # release
        self.lock.release()
        _log.info("(%r) released", self)


def test_threaded_001(*args: Any, **kwargs: Any) -> None:
    """Test a thread acquiring a lock."""
    _log.info("test_threaded_001 %r %r", args, kwargs)

    sync_event = Event()
    _log.info("    - sync_event: %r", sync_event)

    # make a thread using a lock
    lock_thread_1 = ExclusiveThread("test_threaded_001", sync_event)
    _log.info("    - lock_thread_1: %r", lock_thread_1)

    # start it up then spin-wait for the thread to successfully acquire it
    lock_thread_1.start()
    while (lock_owner := r.get("test_threaded_001:exclusive")) is None:
        time.sleep(0.001)
    _log.info("    - lock_owner: %r", lock_owner)
    assert lock_owner.decode() == lock_thread_1.lock._id

    # set the event so the thread can release the lock
    sync_event.set()
    _log.info("    - sync set")

    # wait for the thread to finish
    lock_thread_1.join()
    _log.info("    - lock_thread_1: %r", lock_thread_1)

    # make sure it's gone
    assert r.get("test_threaded_001:exclusive") is None
    assert r.zcount("test_threaded_001:exclusive_waiting", "-inf", "+inf") == 0
    assert r.zcount("test_threaded_001:shared", "-inf", "+inf") == 0


def test_threaded_002(*args: Any, **kwargs: Any) -> None:
    """Two threads contending for the same lock."""
    _log.info("test_threaded_002 %r %r", args, kwargs)

    sync_event = Event()
    _log.info("    - sync_event: %r", sync_event)

    lock_thread_1 = ExclusiveThread("test_threaded_002", sync_event)
    _log.info("    - lock_thread_1: %r", lock_thread_1)

    lock_thread_1.start()
    while (lock_owner := r.get("test_threaded_002:exclusive")) is None:
        time.sleep(0.001)
    _log.info("    - lock_owner: %r", lock_owner)
    assert lock_owner.decode() == lock_thread_1.lock._id

    lock_thread_2 = ExclusiveThread("test_threaded_002", sync_event)
    _log.info("    - lock_thread_1: %r", lock_thread_1)

    lock_thread_2.start()

    sync_event.set()
    _log.info("    - sync set")

    lock_thread_1.join()
    _log.info("    - lock_thread_1: %r", lock_thread_1)
    lock_thread_2.join()
    _log.info("    - lock_thread_2: %r", lock_thread_2)

    # make sure it's gone
    assert r.get("test_threaded_002:exclusive") is None
    assert r.zcount("test_threaded_002:exclusive_waiting", "-inf", "+inf") == 0
    assert r.zcount("test_threaded_002:shared", "-inf", "+inf") == 0
