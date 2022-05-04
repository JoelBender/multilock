# multilock

This Python library is a [Redis](https://redis.io) implementation of a
"multilock" which is similar to locks, but has two different "levels".
Instances of the lock can be acquired _exclusive_ used to block access to a
resource to all other processes or threads, or _shared_ used when a process
or thread needs access to a resource but can shared it with others.  A lock
cannot be acquired _exclusive_ until all other shared or exclusive access
has been released.
