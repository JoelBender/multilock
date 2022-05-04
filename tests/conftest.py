import asyncio
import pytest
import logging

import redis

# logging
_log = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def r():
    _log.info("r -- redis_connection")

    r = redis.from_url("redis://localhost/")
    logging.debug("    - connection: %r", r)

    yield r
    logging.debug("    - finished with the connection")

    r.close()
