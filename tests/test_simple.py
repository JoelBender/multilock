"""
Test Module Template
--------------------
"""

import unittest
import logging
import pytest
import time

from typing import Any

import redis
from multilock import Multilock, CannotObtainLock, LockExpired

# logging
_log = logging.getLogger(__name__)

def test_exclusive_001(r, *args: Any, **kwargs: Any) -> None:
    """Acquire and release an exclusive lock."""
    _log.info("test_exclusive_001 %r %r", args, kwargs)

    lock = Multilock(r, "test_exclusive_001")

    # acquire/release
    lock.acquire_exclusive(1.0)
    lock.release()

    # make sure it's gone
    assert r.get("test_exclusive_001:exclusive") is None
    assert r.zcount("test_exclusive_001:exclusive_waiting", "-inf", "+inf") == 0
    assert r.zcount("test_exclusive_001:shared", "-inf", "+inf") == 0


def test_exclusive_002(r, *args: Any, **kwargs: Any) -> None:
    """Acquire an exclusive lock, blocking another attempt."""
    _log.info("test_exclusive_002 %r %r", args, kwargs)

    lock_1 = Multilock(r, "test_exclusive_002", retry_count=0)
    lock_2 = Multilock(r, "test_exclusive_002", retry_count=0)

    lock_1.acquire_exclusive(0.5)
    lock_owner = r.get("test_exclusive_002:exclusive")
    assert lock_owner.decode() == lock_1._id

    # lock_2 is blocked
    with pytest.raises(CannotObtainLock):
        lock_2.acquire_exclusive(0.5)

    lock_1.release()

    # now this is successful
    lock_2.acquire_exclusive(0.5)
    lock_2.release()

    # make sure it's gone
    assert r.get("test_exclusive_002:exclusive") is None
    assert r.zcount("test_exclusive_002:exclusive_waiting", "-inf", "+inf") == 0
    assert r.zcount("test_exclusive_002:shared", "-inf", "+inf") == 0


def test_exclusive_003(r, *args: Any, **kwargs: Any) -> None:
    """Acquire, refresh, and release an exclusive lock."""
    _log.info("test_exclusive_003 %r %r", args, kwargs)

    lock_1 = Multilock(r, "test_exclusive_003")

    # acquire
    lock_1.acquire_exclusive(1.0)
    lock_owner = r.get("test_exclusive_003:exclusive")
    assert lock_owner.decode() == lock_1._id

    # wait a little bit
    time.sleep(0.5)

    # refresh and check it is still locked
    lock_1.refresh(1.0)
    lock_owner = r.get("test_exclusive_003:exclusive")
    assert lock_owner.decode() == lock_1._id

    # release the lock
    lock_1.release()

    # make sure it's gone
    assert r.get("test_exclusive_003:exclusive") is None
    assert r.zcount("test_exclusive_003:exclusive_waiting", "-inf", "+inf") == 0
    assert r.zcount("test_exclusive_003:shared", "-inf", "+inf") == 0


def test_exclusive_004(r, *args: Any, **kwargs: Any) -> None:
    """Acquire an exclusive lock and let it expire."""
    _log.info("test_exclusive_004 %r %r", args, kwargs)

    lock_1 = Multilock(r, "test_exclusive_004")

    # acquire
    lock_1.acquire_exclusive(0.5)
    lock_owner = r.get("test_exclusive_004:exclusive")
    assert lock_owner.decode() == lock_1._id

    # wait until the lock has expired
    time.sleep(0.5)

    # attempt to refresh
    with pytest.raises(LockExpired):
        lock_1.refresh(0.5)

    # release the lock
    lock_1.release()

    # make sure it's gone
    assert r.get("test_exclusive_004:exclusive") is None
    assert r.zcount("test_exclusive_004:exclusive_waiting", "-inf", "+inf") == 0
    assert r.zcount("test_exclusive_004:shared", "-inf", "+inf") == 0


def test_shared_001(r, *args: Any, **kwargs: Any) -> None:
    """Acquire and release a shared lock."""
    _log.info("test_shared_001 %r %r", args, kwargs)

    lock_1 = Multilock(r, "test_shared_001")

    lock_1.acquire_shared(0.5)

    rank = r.zrank("test_shared_001:shared", lock_1._id)
    _log.debug("    - rank: %r", rank)
    assert rank is not None

    lock_1.release()

    # make sure it's gone
    assert r.get("test_shared_001:exclusive") is None
    assert r.zcount("test_shared_001:exclusive_waiting", "-inf", "+inf") == 0
    assert r.zcount("test_shared_001:shared", "-inf", "+inf") == 0


def test_shared_002(r, *args: Any, **kwargs: Any) -> None:
    """Acquire two shared locks at the same time."""
    _log.info("test_shared_002 %r %r", args, kwargs)

    lock_1 = Multilock(r, "test_shared_002")
    lock_2 = Multilock(r, "test_shared_002")

    lock_1.acquire_shared(0.5)

    rank = r.zrank("test_shared_002:shared", lock_1._id)
    _log.debug("    - lock 1 rank: %r", rank)
    assert rank is not None

    lock_2.acquire_shared(0.5)

    rank = r.zrank("test_shared_002:shared", lock_2._id)
    _log.debug("    - lock 2 rank: %r", rank)
    assert rank is not None

    lock_1.release()
    lock_2.release()

    # make sure it's gone
    assert r.get("test_shared_002:exclusive") is None
    assert r.zcount("test_shared_002:exclusive_waiting", "-inf", "+inf") == 0
    assert r.zcount("test_shared_002:shared", "-inf", "+inf") == 0


def test_shared_003(r, *args: Any, **kwargs: Any) -> None:
    """Acquire, refresh, and release an exclusive lock."""
    _log.info("test_shared_003 %r %r", args, kwargs)

    lock_1 = Multilock(r, "test_shared_003")

    # acquire
    lock_1.acquire_shared(1.0)
    rank = r.zrank("test_shared_003:shared", lock_1._id)
    _log.debug("    - initial rank: %r", rank)
    assert rank is not None

    # wait a little bit
    time.sleep(0.5)

    # refresh and check it is still locked
    lock_1.refresh(1.0)
    rank = r.zrank("test_shared_003:shared", lock_1._id)
    _log.debug("    - refreshed rank: %r", rank)
    assert rank is not None

    # release the lock
    lock_1.release()

    # make sure it's gone
    assert r.get("test_shared_003:exclusive") is None
    assert r.zcount("test_shared_003:exclusive_waiting", "-inf", "+inf") == 0
    assert r.zcount("test_shared_003:shared", "-inf", "+inf") == 0


def test_shared_004(r, *args: Any, **kwargs: Any) -> None:
    """Acquire, refresh, and release a shared lock."""
    _log.info("test_shared_004 %r %r", args, kwargs)

    lock_1 = Multilock(r, "test_shared_004")

    # acquire
    lock_1.acquire_shared(0.5)
    rank = r.zrank("test_shared_004:shared", lock_1._id)
    _log.debug("    - initial rank: %r", rank)
    assert rank is not None

    # wait a little bit
    time.sleep(0.5)

    # attempt to refresh, it has expired
    with pytest.raises(LockExpired):
        lock_1.refresh(0.5)

    # release the lock
    lock_1.release()

    # make sure it's gone
    assert r.get("test_shared_004:exclusive") is None
    assert r.zcount("test_shared_004:exclusive_waiting", "-inf", "+inf") == 0
    assert r.zcount("test_shared_004:shared", "-inf", "+inf") == 0
