"""
Distributed locks with Redis
Redis doc: http://redis.io/topics/distlock
"""
from __future__ import division
from datetime import datetime
import random
import time
import uuid

import redis


DEFAULT_RETRY_TIMES = 3
DEFAULT_RETRY_DELAY = 200
DEFAULT_TTL = 100000
CLOCK_DRIFT_FACTOR = 0.01

# Reference:  http://redis.io/topics/distlock
# Section Correct implementation with a single instance
RELEASE_LUA_SCRIPT = """
    if redis.call("get",KEYS[1]) == ARGV[1] then
        return redis.call("del",KEYS[1])
    else
        return 0
    end
"""

GET_TTL_LUA_SCRIPT = """
    if redis.call("get",KEYS[1]) == ARGV[1] then
        return redis.call("pttl",KEYS[1])
    else
        return 0
    end
"""

# Reference:  http://redis.io/topics/distlock
# Section Making the algorithm more reliable: Extending the lock
BUMP_LUA_SCRIPT = """
    if redis.call("get",KEYS[1]) == ARGV[1] then
        return redis.call("pexpire",KEYS[1],ARGV[2])
    else
        return 0
    end
"""


class RedLockError(Exception):
    pass


class RedLockFactory(object):

    """
    A Factory class that helps reuse multiple Redis connections.
    """

    def __init__(self, connection_details):
        """
        Create a new RedLockFactory
        """
        self.redis_nodes = []

        for conn in connection_details:
            if isinstance(conn, redis.StrictRedis):
                node = conn
            elif 'url' in conn:
                url = conn.pop('url')
                node = redis.StrictRedis.from_url(url, **conn)
            else:
                node = redis.StrictRedis(**conn)
            node._release_script = node.register_script(RELEASE_LUA_SCRIPT)
            node._bump_script = node.register_script(BUMP_LUA_SCRIPT)
            node._get_ttl_script = node.register_script(GET_TTL_LUA_SCRIPT)
            self.redis_nodes.append(node)
            self.quorum = len(self.redis_nodes) // 2 + 1

    def create_lock(self, resource, **kwargs):
        """
        Create a new RedLock object and reuse stored Redis clients.
        All the kwargs it received would be passed to the RedLock's __init__
        function.
        """
        lock = RedLock(resource=resource, created_by_factory=True, **kwargs)
        lock.redis_nodes = self.redis_nodes
        lock.quorum = self.quorum
        lock.factory = self
        return lock


class RedLock(object):

    """
    A distributed lock implementation based on Redis.
    It shares a similar API with the `threading.Lock` class in the
    Python Standard Library.
    """

    def __init__(self, resource, connection_details=None,
                 retry_times=DEFAULT_RETRY_TIMES,
                 retry_delay=DEFAULT_RETRY_DELAY,
                 ttl=DEFAULT_TTL,
                 created_by_factory=False):
        """
        Create a new RedLock
        """

        self.lock_key = None
        self.resource = resource
        self.retry_times = retry_times
        self.retry_delay = retry_delay
        self.ttl = ttl

        if created_by_factory:
            self.factory = None
            return

        self.redis_nodes = []
        # If the connection_details parameter is not provided,
        # use redis://127.0.0.1:6379/0
        if connection_details is None:
            connection_details = [{
                'host': 'localhost',
                'port': 6379,
                'db': 0,
            }]

        for conn in connection_details:
            if isinstance(conn, redis.StrictRedis):
                node = conn
            elif 'url' in conn:
                url = conn.pop('url')
                node = redis.StrictRedis.from_url(url, **conn)
            else:
                node = redis.StrictRedis(**conn)
            node._release_script = node.register_script(RELEASE_LUA_SCRIPT)
            node._bump_script = node.register_script(BUMP_LUA_SCRIPT)
            node._get_ttl_script = node.register_script(GET_TTL_LUA_SCRIPT)
            self.redis_nodes.append(node)
        self.quorum = len(self.redis_nodes) // 2 + 1

    def __enter__(self):
        acquired, validity = self.acquire_with_validity()
        if not acquired:
            raise RedLockError('failed to acquire lock')
        return validity

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    def _total_ms(self, delta):
        """
        Get the total number of milliseconds in a timedelta object with
        microsecond precision.
        """
        delta_seconds = delta.seconds + delta.days * 24 * 3600
        return (delta.microseconds + delta_seconds * 10**6) / 10**3

    def acquire_node(self, node):
        """
        acquire a single redis node
        """
        try:
            return node.set(self.resource, self.lock_key, nx=True, px=self.ttl)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            return False

    def release_node(self, node):
        """
        release a single redis node
        """
        # use the lua script to release the lock in a safe way
        try:
            return node._release_script(keys=[self.resource], args=[self.lock_key])
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            pass

    def bump_node(self, node):
        """
        reset the ttl on a single redis node
        """
        try:
            return node._bump_script(keys=[self.resource], args=[self.lock_key, self.ttl])
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            pass

    def get_ttl_from_node(self, node):
        """
        get the ttl of the key from a single node
        """
        try:
            return node._get_ttl_script(keys=[self.resource], args=[self.lock_key])
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            pass

    def acquire(self):
        """
        Attempt to acquire a new lock
        """
        acquired, validity = self._acquire()
        return acquired

    def acquire_with_validity(self):
        """
        Attempt to acquire a new lock, and return lock validity time
        """
        return self._acquire()

    def _acquire(self):
        for retry in range(self.retry_times + 1):
            # Hold onto the previous lock_key, just in case this
            # acquire fails this Lock could still possibly be used
            # to .extend() the lock it holds, or to reacquire it
            # later after it has expired or had .release() called
            previous_lock_key = self.lock_key
            self.lock_key = uuid.uuid4().hex
            acquired_node_count = 0
            start_time = datetime.utcnow()

            # acquire the lock in all the redis instances sequentially
            for node in self.redis_nodes:
                if self.acquire_node(node):
                    acquired_node_count += 1

            end_time = datetime.utcnow()
            elapsed_milliseconds = self._total_ms(end_time - start_time)

            # Add 2 milliseconds to the drift to account for Redis expires
            # precision, which is 1 milliescond, plus 1 millisecond min drift
            # for small TTLs.
            drift = (self.ttl * CLOCK_DRIFT_FACTOR) + 2

            validity = self.ttl - (elapsed_milliseconds + drift)
            if acquired_node_count >= self.quorum and validity > 0:
                return True, validity
            else:
                # We didn't get the lock, be nice and release all
                # the nodes, then sleep for a bit and retry if
                # possible, else return False, 0
                for node in self.redis_nodes:
                    self.release_node(node)
                self.lock_key = previous_lock_key
                time.sleep(random.randint(0, self.retry_delay) / 1000)
        return False, 0

    def check(self):
        """
        Check to see if the lock is still held
        """
        return self.check_times()[0]

    def check_times(self):
        """
        Return whether or not the lock is still held,
        and how long each node reports the lock will be
        held for, accounting for query time and clock drift
        """
        times = []
        start_time = datetime.utcnow()
        for node in self.redis_nodes:
            y = self.get_ttl_from_node(node)
            if y > 0:
                times.append(y)
        end_time = datetime.utcnow()
        drift = (self.ttl * CLOCK_DRIFT_FACTOR) + 2
        elapsed_milliseconds = self._total_ms(end_time - start_time)
        # Compute times taking into account how long it took to query all
        # the nodes as well as clock drift constant. Sort out any negative
        # times (keys that may have expired while we were querying other nodes).
        times = [x-(elapsed_milliseconds+drift) for x in times
                 if x-(elapsed_milliseconds+drift) > 0]
        if len(times) > 0:
            return min(times) > 0 and len(times) >= self.quorum, times
        return False, []

    def release(self):
        """
        Release the lock

        We can return whether or not we are sure the lock was actually
        released by checking to see if the command output from a quorum
        of the redis nodes, and return True/False based on that.
        """
        released = []
        for node in self.redis_nodes:
            released.append(self.release_node(node))
        return len([x for x in released if x is not None]) >= self.quorum

    def extend(self):
        """
        Extend the lifespan of the lock by returning its ttl to the original value
        """
        extended, validity = self._extend()
        return extended

    def extend_with_validity(self):
        """
        Extend the lifespan of the lock by returning its ttl to the original value

        Also return the ttl of the lock
        """
        return self._extend()

    def _extend(self):
        # Returns whether or not the lock was extended succesfully -
        # not whether or not the lock is held, which could conceivably
        # be different values in strange cases.
        # Extended implies held, held does not imply extended.
        # The second entry in the returned tuple is the validity time
        # if the lock was successfully extended or None if it wasn't.
        # See "Making the algorithm more reliable: Extending the lock" here:
        # https://redis.io/topics/distlock
        for retry in range(self.retry_times + 1):
            bumps = 0
            start_time = datetime.utcnow()
            for node in self.redis_nodes:
                if self.bump_node(node):
                    bumps += 1
            end_time = datetime.utcnow()
            elapsed_milliseconds = self._total_ms(end_time - start_time)
            drift = (self.ttl * CLOCK_DRIFT_FACTOR) + 2
            validity = self.ttl - (elapsed_milliseconds + drift)
            if bumps >= self.quorum and validity > 0:
                return True, validity
            time.sleep(random.randint(0, self.retry_delay) / 1000)
        return False, None


class ReentrantRedLock(RedLock):
    def __init__(self, *args, **kwargs):
        super(ReentrantRedLock, self).__init__(*args, **kwargs)
        self._acquired = 0

    def acquire(self):
        # Note: This function now checks the lock before calls when
        # self._aquired > 0.
        # If the lock has been lost it resets the acquired counter to 0 and
        # automatically attempts to reacquire the lock. This means that
        # number of calls to acquire == number of calls to release iff they
        # are all made during the same lock lifespan. Eg the following would
        # be possible
        #
        # >>> relock.acquire()
        # True
        # *the lock ttl goes by, we lose the lock without calling .release()*
        # >>> relock.acquire()
        # True
        # >>> relock.release()
        # True
        # * the lock is actually released after the first call, because the *
        # * first acquire call timed out *
        #
        # This is preferable, in my opinion, to calls to .acquire() returning True
        # when the lock isn't actually held because it timed out.
        if self._acquired > 0:
            if self.check() is False:
                self._acquired = 0
            else:
                self._acquired += 1
                return True

        if self._acquired == 0:
            result = super(ReentrantRedLock, self).acquire()
            if result is True:
                self._acquired += 1
            return result

    def acquire_and_extend(self):
        if self.extend() is True:
            self._acquired += 1
            return True
        else:
            if self.acquire():
                return True
        return False

    def release(self):
        if self._acquired > 0:
            self._acquired -= 1
            if self._acquired == 0:
                return super(ReentrantRedLock, self).release()
