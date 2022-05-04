import time
import uuid
import logging

from enum import Enum

import redis

# references to compiled/installed scripts
acquire_exclusive_lua_script = None
refresh_exclusive_lua_script = None
release_exclusive_lua_script = None
acquire_shared_lua_script = None
refresh_shared_lua_script = None
release_shared_lua_script = None
flush_lua_script = None


class LockState(Enum):
    IDLE = 1
    WAITING_EXCLUSIVE = 2
    ACQUIRED_EXCLUSIVE = 3
    WAITING_SHARED = 4
    ACQUIRED_SHARED = 5


class CannotObtainLock(Exception):
    """
    Exception is raised when the acquire_x() calls fail.
    """

    pass


class LockExpired(Exception):
    """
    Exception is raised when attempting to refresh a lock and it has already
    expired.
    """

    pass


acquire_exclusive_lua = """
    local lockname = ARGV[1]
    local identifier = ARGV[2]
    local exclusive = lockname .. ':exclusive'
    local exclusive_waiting = lockname .. ':exclusive_waiting'
    local shared = lockname .. ':shared'
    local shared_waiting = lockname .. ':shared_waiting'

    local now = tonumber(ARGV[3])
    local timeout = tonumber(ARGV[4])
    local retcode = 99

    -- timeout old entries
    redis.call("ZREMRANGEBYSCORE", exclusive_waiting, "-inf", now - timeout)
    redis.call("ZREMRANGEBYSCORE", shared, "-inf", now - timeout)
    redis.call("ZREMRANGEBYSCORE", shared_waiting, "-inf", now - timeout)

    redis.call("ZADD", exclusive_waiting, now, identifier)
    if redis.call("ZRANK", exclusive_waiting, identifier) == 0 then
        -- we are first in line if it is available

        local shared_count = redis.call("ZCOUNT", shared, "-inf", "+inf")

        if shared_count > 0 then
            -- others have it shared
            retcode = 1
        else
            if redis.call("get", exclusive) then
                -- somebody else already has it exclusive
                retcode = 1
            else
                -- nobody else is waiting
                redis.call("ZREM", exclusive_waiting, identifier)
                redis.call("SET", exclusive, identifier, "PX", timeout * 1000)
                retcode = 0
            end
        end
    else
        -- we are queued behind somebody else
        retcode = 1
    end

    return retcode
    """

refresh_exclusive_lua = """
    local lockname = ARGV[1]
    local identifier = ARGV[2]
    local exclusive = lockname .. ':exclusive'
    local exclusive_waiting = lockname .. ':exclusive_waiting'
    local shared = lockname .. ':shared'
    local shared_waiting = lockname .. ':shared_waiting'

    local now = tonumber(ARGV[3])
    local timeout = tonumber(ARGV[4])
    local retcode = 0

    if redis.call("GET", exclusive) == identifier then
        redis.call("SET", exclusive, identifier, "PX", timeout * 1000)
    else
        retcode = 1
    end

    return retcode
    """

release_exclusive_lua = """
    local lockname = ARGV[1]
    local identifier = ARGV[2]
    local exclusive = lockname .. ':exclusive'
    local exclusive_waiting = lockname .. ':exclusive_waiting'

    if redis.call("GET", exclusive) == identifier then
        redis.call("DEL", exclusive)
        return 0
    else
        -- maybe waiting
        return redis.call("ZREM", exclusive_waiting, identifier)
    end
    """

acquire_shared_lua = """
    local lockname = ARGV[1]
    local identifier = ARGV[2]
    local exclusive = lockname .. ':exclusive'
    local exclusive_waiting = lockname .. ':exclusive_waiting'
    local shared = lockname .. ':shared'
    local shared_waiting = lockname .. ':shared_waiting'

    local now = tonumber(ARGV[3])
    local timeout = tonumber(ARGV[4])
    local retcode = 99

    -- timeout old entries
    redis.call("ZREMRANGEBYSCORE", exclusive_waiting, "-inf", now - timeout)
    redis.call("ZREMRANGEBYSCORE", shared, "-inf", now - timeout)
    redis.call("ZREMRANGEBYSCORE", shared_waiting, "-inf", now - timeout)

    redis.call("ZADD", shared_waiting, now, identifier)
    if redis.call("GET", exclusive) then
        -- somebody already has it exclusive
        retcode = 2
    else
        local exclusive_waiting_count = redis.call("ZCOUNT", exclusive_waiting, "-inf", "+inf")
        if exclusive_waiting_count > 0 then
            -- some one or more is waiting for exclusive
            retcode = 2
        else
            redis.call("ZREM", shared_waiting, identifier)
            redis.call("ZADD", shared, now, identifier)
            retcode = 0
        end
    end

    return retcode
    """

refresh_shared_lua = """
    local lockname = ARGV[1]
    local identifier = ARGV[2]
    local shared = lockname .. ':shared'

    local now = tonumber(ARGV[3])
    local timeout = tonumber(ARGV[4])
    local retcode = 0

    redis.call("ZREMRANGEBYSCORE", shared, "-inf", now - timeout)
    if redis.call("ZADD", shared, "XX", "CH", now, identifier) ~= 1 then
        retcode = 1
    end

    return retcode
    """

release_shared_lua = """
    local lockname = ARGV[1]
    local identifier = ARGV[2]
    local shared = lockname .. ':shared'
    local shared_waiting = lockname .. ':shared_waiting'
    local retcode = 99

    -- normally have it
    if redis.call("ZREM", shared, identifier) == 1 then
        retcode = 0
    else
        -- maybe waiting
        if redis.call("ZREM", shared_waiting, identifier) == 1 then
            retcode = 0
        else
            retcode = 1
        end
    end

    return retcode
    """

flush_lua = """
    local lockname = ARGV[1]
    local exclusive = lockname .. ':exclusive'
    local exclusive_waiting = lockname .. ':exclusive_waiting'
    local shared = lockname .. ':shared'
    local shared_waiting = lockname .. ':shared_waiting'
    local retcode = 0

    redis.call("DEL", exclusive)
    redis.call("DEL", exclusive_waiting)
    redis.call("DEL", shared)
    redis.call("DEL", shared_waiting)

    return retcode
    """


def register_scripts(r: redis.Redis) -> None:
    global acquire_exclusive_lua_script, refresh_exclusive_lua_script, release_exclusive_lua_script
    global acquire_shared_lua_script, refresh_shared_lua_script, release_shared_lua_script
    global flush_lua_script

    acquire_exclusive_lua_script = r.register_script(acquire_exclusive_lua)
    logging.debug(
        "    - acquire_exclusive_lua_script: %r", acquire_exclusive_lua_script
    )

    refresh_exclusive_lua_script = r.register_script(refresh_exclusive_lua)
    logging.debug(
        "    - refresh_exclusive_lua_script: %r", refresh_exclusive_lua_script
    )

    release_exclusive_lua_script = r.register_script(release_exclusive_lua)
    logging.debug(
        "    - release_exclusive_lua_script: %r", release_exclusive_lua_script
    )

    acquire_shared_lua_script = r.register_script(acquire_shared_lua)
    logging.debug("    - acquire_shared_lua_script: %r", acquire_shared_lua_script)

    refresh_shared_lua_script = r.register_script(refresh_shared_lua)
    logging.debug("    - refresh_shared_lua_script: %r", refresh_shared_lua_script)

    release_shared_lua_script = r.register_script(release_shared_lua)
    logging.debug("    - release_shared_lua_script: %r", release_shared_lua_script)

    flush_lua_script = r.register_script(flush_lua)
    logging.debug("    - flush_lua_script: %r", flush_lua_script)


class Multilock(object):

    default_retry_count = 3
    default_retry_delay = 0.2
    clock_drift_factor = 0.01
    transition_delay = 0.01

    def __init__(
        self, redis: redis.Redis, name: str, retry_count=None, retry_delay=None
    ):
        logging.debug(
            "__init__ %r %r retry_count=%r retry_delay=%r",
            redis,
            name,
            retry_count,
            retry_delay,
        )
        global acquire_exclusive_lua_script

        # make sure the scripts are registered
        if not acquire_exclusive_lua_script:
            register_scripts(redis)

        # connection, name, and other settings
        self.redis = redis
        self.name = name
        self.retry_count = (
            retry_count if retry_count is not None else self.default_retry_count
        )
        self.retry_delay = (
            retry_delay if retry_delay is not None else self.default_retry_delay
        )

        # instance identity assigned for acquired
        self._id = None
        self._state = LockState.IDLE

    def acquire_exclusive(self, ttl: float, retry_count=None, retry_delay=None):
        logging.debug(
            "acquire_exclusive %r retry_count=%r retry_delay=%r",
            ttl,
            retry_count,
            retry_delay,
        )
        logging.debug("    - lock state: %r", self._state)

        if self._state == LockState.IDLE:
            self._id = str(uuid.uuid4())
            logging.debug("    - id: %r", self._id)
        elif self._state == LockState.WAITING_EXCLUSIVE:
            logging.debug("    - current id: %r", self._id)
        elif self._state != LockState.IDLE:
            raise RuntimeError(f"invalid lock state: {self._state}")

        if retry_count is None:
            retry_count = self.retry_count
        logging.debug("    - retry_count: %r", retry_count)
        if retry_delay is None:
            retry_delay = self.retry_delay
        logging.debug("    - retry_delay: %r", retry_delay)

        now = time.time()
        logging.debug("    - now: %r", now)

        retry = 0
        while retry <= retry_count:
            retcode = acquire_exclusive_lua_script(args=[self.name, self._id, now, ttl])
            logging.debug("    - retcode: %r", retcode)

            # success
            if retcode == 0:
                self._state = LockState.ACQUIRED_EXCLUSIVE
                return

            # queued behind other exclusive or shared users
            self._state = LockState.WAITING_EXCLUSIVE
            time.sleep(retry_delay)
            retry += 1

        logging.debug("    - nope")
        raise CannotObtainLock(self.name)

    def acquire_shared(self, ttl: float, retry_count=None, retry_delay=None):
        logging.debug(
            "acquire_shared %r retry_count=%r retry_delay=%r",
            ttl,
            retry_count,
            retry_delay,
        )
        logging.debug("    - lock state: %r", self._state)

        if self._state == LockState.IDLE:
            self._id = str(uuid.uuid4())
            logging.debug("    - id: %r", self._id)
        elif self._state == LockState.WAITING_SHARED:
            logging.debug("    - current id: %r", self._id)
        elif self._state != LockState.IDLE:
            raise RuntimeError(f"invalid lock state: {self._state}")

        if retry_count is None:
            retry_count = self.retry_count
        logging.debug("    - retry_count: %r", retry_count)
        if retry_delay is None:
            retry_delay = self.retry_delay
        logging.debug("    - retry_delay: %r", retry_delay)

        now = time.time()
        logging.debug("    - now: %r", now)

        retry = 0
        while retry <= retry_count:
            retcode = acquire_shared_lua_script(args=[self.name, self._id, now, ttl])
            logging.debug("    - retcode: %r", retcode)

            # success
            if retcode == 0:
                self._state = LockState.ACQUIRED_SHARED
                return

            # queued behind other exclusive or shared users
            self._state = LockState.WAITING_SHARED
            time.sleep(retry_delay)
            retry += 1

        logging.debug("    - nope")
        raise CannotObtainLock(self.name)

    def refresh(self, ttl: float) -> None:
        global refresh_exclusive_lua_script, refresh_shared_lua_script

        logging.debug("refresh %r", ttl)
        logging.debug("    - lock state: %r", self._state)

        now = time.time()
        logging.debug("    - now: %r", now)

        if self._state == LockState.ACQUIRED_EXCLUSIVE:
            retcode = refresh_exclusive_lua_script(args=[self.name, self._id, now, ttl])
            logging.debug("    - retcode: %r", retcode)

            if (retcode == 1):
                raise LockExpired(self.name)

        elif self._state == LockState.ACQUIRED_SHARED:
            retcode = refresh_shared_lua_script(args=[self.name, self._id, now, ttl])
            logging.debug("    - retcode: %r", retcode)

            if (retcode == 1):
                raise LockExpired(self.name)

        else:
            raise RuntimeError(f"invalid lock state: {self._state}")

    def release(self) -> None:
        global release_exclusive_lua_script

        logging.debug("release")
        logging.debug("    - lock state: %r", self._state)

        if self._state == LockState.IDLE:
            logging.debug("    - already idle")

        elif (self._state == LockState.WAITING_EXCLUSIVE) or (
            self._state == LockState.ACQUIRED_EXCLUSIVE
        ):
            retcode = release_exclusive_lua_script(args=[self.name, self._id])
            logging.debug("    - retcode: %r", retcode)

            if retcode == 0:
                logging.debug("    - it was ours")
            elif retcode == 1:
                logging.debug("    - owned by someone else")

        elif (self._state == LockState.WAITING_SHARED) or (
            self._state == LockState.ACQUIRED_SHARED
        ):
            retcode = release_shared_lua_script(args=[self.name, self._id])
            logging.debug("    - retcode: %r", retcode)

            if retcode == 0:
                logging.debug("    - it was ours")
            elif retcode == 1:
                logging.debug("    - owned by someone else")

        self._id = None
        self._state = LockState.IDLE

    def flush(self) -> None:
        logging.debug(
            "flush"
        )
        if self._state != LockState.IDLE:
            raise RuntimeError(f"invalid lock state: {self._state}")

        retcode = flush_lua_script(args=[self.name])
        logging.debug("    - retcode: %r", retcode)
