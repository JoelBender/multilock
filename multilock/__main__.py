"""
Commands to acquire/release locks, flush them, and get information about them.
"""

import sys
import time
import logging
import argparse

from typing import Optional

import redis
from multilock import Multilock, CannotObtainLock


#
#   __main__
#


def main(command_line: Optional[str] = None) -> int:
    # common arguments and options
    parser = argparse.ArgumentParser("multilock")
    parser.add_argument("--debug", action="store_true", help="Print debug info")
    parser.add_argument(
        "-r",
        "--redis",
        help="connection string",
        default="redis://localhost/",
        nargs="?",
    )
    subparsers = parser.add_subparsers(dest="command")

    # exclusive subcommand
    exclusive = subparsers.add_parser("exclusive", help="acquire exclusive")
    exclusive.add_argument("name", help="lock name")
    exclusive.add_argument("-t", "--ttl", type=float, default=1.0, help="lock time")
    exclusive.add_argument("-c", "--retry_count", type=int, help="retry count")
    exclusive.add_argument(
        "-d", "--retry_delay", type=float, help="delay between retries"
    )

    # shared subcommand
    shared = subparsers.add_parser("shared", help="acquire shared")
    shared.add_argument("name", help="lock name")
    shared.add_argument("-t", "--ttl", type=float, default=1.0, help="lock time")
    shared.add_argument("-c", "--retry_count", type=int, help="retry count")
    shared.add_argument("-d", "--retry_delay", type=float, help="delay between retries")

    # flush subcommand
    flush = subparsers.add_parser("flush", help="flush a lock")
    flush.add_argument("name", help="lock name")

    # flush subcommand
    info = subparsers.add_parser("info", help="information about a lock")
    info.add_argument("name", help="lock name")

    # run the parser
    args = parser.parse_args(command_line)

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    logging.debug("satus")
    logging.debug("    - args: %r", args)

    r = redis.from_url(args.redis)
    logging.debug("    - connection: %r", r)

    lock = Multilock(r, args.name)
    logging.debug("    - lock: %r", lock)

    # assume success
    retcode = 0

    if args.command == "exclusive":
        logging.debug("    - exclusive")
        try:
            lock.acquire_exclusive(
                args.ttl, retry_count=args.retry_count, retry_delay=args.retry_delay
            )
            time.sleep(args.ttl)
            lock.release()
        except CannotObtainLock:
            logging.debug("    - failed")
            retcode = 1

    elif args.command == "shared":
        logging.debug("    - shared")
        try:
            lock.acquire_shared(
                args.ttl, retry_count=args.retry_count, retry_delay=args.retry_delay
            )
            time.sleep(args.ttl)
            lock.release()
        except CannotObtainLock:
            logging.debug("    - failed")
            retcode = 1

    elif args.command == "flush":
        logging.debug("    - flush: %r", args.name)
        lock.flush()

    elif args.command == "info":
        logging.debug("    - info: %r", args.name)
        print(f"exclusive: {r.get(args.name + ':exclusive')!r}")
        print(
            f"exclusive waiting: {r.zrange(args.name + ':exclusive_waiting', 0, -1)!r}"
        )
        print(f"shared: {r.zrange(args.name + ':shared', 0, -1)!r}")
        print(f"shared waiting: {r.zrange(args.name + ':shared_waiting', 0, -1)!r}")

    r.close()
    logging.debug("fini")

    return retcode


if __name__ == "__main__":
    sys.exit(main())
