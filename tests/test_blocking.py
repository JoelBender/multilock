"""
Test Module Template
--------------------
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

def debug_lock(r, name: str, step: int) -> None:
    """Handy function for debugging tests."""
    _log.debug("    dump %d:", step)
    _log.debug("        - exclusive: %r", r.get(name + ":exclusive"))
    _log.debug(
        "        - exclusive waiting: %r", r.zrange(name + ":exclusive_waiting", 0, -1)
    )
    _log.debug("        - shared: %r", r.zrange(name + ":shared", 0, -1))
    _log.debug(
        "        - shared waiting: %r", r.zrange(name + ":shared_waiting", 0, -1)
    )


def test_blocking_004(r, *args: Any, **kwargs: Any) -> None:
    """Exclusive lock blocking shared lock."""
    _log.info("test_shared_002 %r %r", args, kwargs)

    lock_1 = Multilock(r, "test_blocking_004", retry_count=0)
    lock_2 = Multilock(r, "test_blocking_004", retry_count=0)

    # lock 1 is exclusive
    lock_1.acquire_exclusive(0.5)
    lock_owner = r.get("test_blocking_004:exclusive")
    assert lock_owner.decode() == lock_1._id

    # lock 2 is blocked
    with pytest.raises(CannotObtainLock):
        lock_2.acquire_shared(0.5)

    # release lock 1
    lock_1.release()

    # retry lock 2 successful
    lock_2.acquire_shared(0.5)
    rank = r.zrank("test_blocking_004:shared", lock_2._id)
    _log.debug("    - lock 2 rank: %r", rank)
    assert rank is not None

    # release lock 2
    lock_2.release()

    # make sure it's gone
    assert r.get("test_blocking_004:exclusive") is None
    assert r.zcount("test_blocking_004:exclusive_waiting", "-inf", "+inf") == 0
    assert r.zcount("test_blocking_004:shared", "-inf", "+inf") == 0


def test_blocking_005(r, *args: Any, **kwargs: Any) -> None:
    """Shared lock blocking exclusive lock."""
    _log.info("test_blocking_005 %r %r", args, kwargs)

    lock_1 = Multilock(r, "test_blocking_005", retry_count=0)
    lock_2 = Multilock(r, "test_blocking_005", retry_count=0)

    # lock 1 is shared
    lock_1.acquire_shared(0.5)
    rank = r.zrank("test_blocking_005:shared", lock_1._id)
    _log.debug("    - lock 1 rank: %r", rank)

    # lock 2 is exclusive, blocked
    with pytest.raises(CannotObtainLock):
        lock_2.acquire_exclusive(0.5)
    rank = r.zrank("test_blocking_005:exclusive_waiting", lock_2._id)
    _log.debug("    - lock 2 rank: %r", rank)

    # release lock 1
    lock_1.release()

    # retry lock 2 is now successful
    lock_2.acquire_exclusive(0.5)
    lock_owner = r.get("test_blocking_005:exclusive")
    assert lock_owner.decode() == lock_2._id

    # release lock 2
    lock_2.release()

    # make sure it's gone
    assert r.get("test_blocking_005:exclusive") is None
    assert r.zcount("test_blocking_005:exclusive_waiting", "-inf", "+inf") == 0
    assert r.zcount("test_blocking_005:shared", "-inf", "+inf") == 0


def test_blocking_006(r, *args: Any, **kwargs: Any) -> None:
    """Shared locks blocking exclusive lock."""
    _log.info("test_blocking_006 %r %r", args, kwargs)

    lock_1 = Multilock(r, "test_blocking_006", retry_count=0)
    lock_2 = Multilock(r, "test_blocking_006", retry_count=0)
    lock_3 = Multilock(r, "test_blocking_006", retry_count=0)

    # lock 1 is shared
    lock_1.acquire_shared(0.5)
    rank = r.zrank("test_blocking_006:shared", lock_1._id)
    _log.debug("    - lock 1 shared rank: %r", rank)

    # lock 2 is shared with lock 1
    lock_2.acquire_shared(0.5)
    rank = r.zrank("test_blocking_006:shared", lock_2._id)
    _log.debug("    - lock 2 shared rank: %r", rank)

    # lock 3 is exclusive, blocked
    with pytest.raises(CannotObtainLock):
        lock_3.acquire_exclusive(0.5)
    rank = r.zrank("test_blocking_006:exclusive_waiting", lock_3._id)
    _log.debug("    - lock 3 exclusive waiting rank: %r", rank)

    # release lock 1
    lock_1.release()

    # lock 2 still locked
    rank = r.zrank("test_blocking_006:shared", lock_2._id)
    _log.debug("    - lock 2 shared rank: %r", rank)

    # lock 3 is still blocked
    with pytest.raises(CannotObtainLock):
        lock_3.acquire_exclusive(0.5)

    # release lock 2
    lock_2.release()

    # retry lock 3 is now successful
    lock_3.acquire_exclusive(0.5)
    lock_owner = r.get("test_blocking_006:exclusive")
    assert lock_owner.decode() == lock_3._id

    # release lock 3
    lock_3.release()

    # make sure it's gone
    assert r.get("test_blocking_006:exclusive") is None
    assert r.zcount("test_blocking_006:exclusive_waiting", "-inf", "+inf") == 0
    assert r.zcount("test_blocking_006:shared", "-inf", "+inf") == 0


def test_blocking_007(r, *args: Any, **kwargs: Any) -> None:
    """Shared lock blocking exclusive lock, which is blocking another request
    for a shared lock."""
    _log.info("test_blocking_007 %r %r", args, kwargs)

    lock_1 = Multilock(r, "test_blocking_007", retry_count=0)
    lock_2 = Multilock(r, "test_blocking_007", retry_count=0)
    lock_3 = Multilock(r, "test_blocking_007", retry_count=0)

    # lock 1 is shared
    lock_1.acquire_shared(0.5)
    rank = r.zrank("test_blocking_007:shared", lock_1._id)
    _log.debug("    - lock 1 shared rank: %r", rank)

    # lock 2 is exclusive, blocked
    with pytest.raises(CannotObtainLock):
        lock_2.acquire_exclusive(0.5)
    rank = r.zrank("test_blocking_007:exclusive_waiting", lock_2._id)
    _log.debug("    - lock 2 exclusive waiting rank: %r", rank)

    # lock 3 is shared, blocked
    with pytest.raises(CannotObtainLock):
        lock_3.acquire_shared(0.5)
    rank = r.zrank("test_blocking_007:shared_waiting", lock_3._id)
    _log.debug("    - lock 3 shared waiting rank: %r", rank)

    # release lock 1
    lock_1.release()

    # retry lock 2 is successful
    lock_2.acquire_exclusive(0.5)
    lock_owner = r.get("test_blocking_007:exclusive")
    assert lock_owner.decode() == lock_2._id

    # lock 3 is still blocked
    with pytest.raises(CannotObtainLock):
        lock_3.acquire_shared(0.5)
    rank = r.zrank("test_blocking_007:shared_waiting", lock_3._id)
    _log.debug("    - lock 3 shared waiting rank: %r", rank)

    # release lock 2
    lock_2.release()

    # retry lock 3 successful
    lock_3.acquire_shared(0.5)
    rank = r.zrank("test_blocking_007:shared", lock_3._id)
    _log.debug("    - lock 3 shared rank: %r", rank)

    # release lock 3
    lock_3.release()

    # make sure it's gone
    assert r.get("test_blocking_007:exclusive") is None
    assert r.zcount("test_blocking_007:exclusive_waiting", "-inf", "+inf") == 0
    assert r.zcount("test_blocking_007:shared", "-inf", "+inf") == 0
