from redlock import RedLock, ReentrantRedLock, RedLockError
from redlock.lock import CLOCK_DRIFT_FACTOR
import time


def test_default_connection_details_value():
    """
    Test that RedLock instance could be created with
    default value of `connection_details` argument.
    """
    lock = RedLock("test_simple_lock")


def test_simple_lock():
    """
    Test a RedLock can be acquired.
    """
    lock = RedLock("test_simple_lock", [{"host": "localhost"}], ttl=1000)
    locked = lock.acquire()
    lock.release()
    assert locked == True


def test_lock_with_validity():
    """
    Test a RedLock can be acquired and the lock validity is also retruned.
    """
    ttl = 1000
    lock = RedLock("test_simple_lock", [{"host": "localhost"}], ttl=ttl)
    locked, validity = lock.acquire_with_validity()
    lock.release()
    assert locked == True
    assert 0 < validity < ttl - ttl * CLOCK_DRIFT_FACTOR - 2


def test_from_url():
    """
    Test a RedLock can be acquired via from_url.
    """
    lock = RedLock("test_from_url", [{"url": "redis://localhost/0"}], ttl=1000)
    locked = lock.acquire()
    lock.release()
    assert locked == True


def test_context_manager():
    """
    Test a RedLock can be released by the context manager automically.

    """
    ttl = 1000
    with RedLock("test_context_manager", [{"host": "localhost"}], ttl=ttl) as validity:
        assert 0 < validity < ttl - ttl * CLOCK_DRIFT_FACTOR - 2
        lock = RedLock("test_context_manager", [{"host": "localhost"}], ttl=ttl)
        locked = lock.acquire()
        assert locked == False

    lock = RedLock("test_context_manager", [{"host": "localhost"}], ttl=ttl)
    locked = lock.acquire()
    assert locked == True

    # try to lock again within a with block
    try:
        with RedLock("test_context_manager", [{"host": "localhost"}]):
            # shouldn't be allowed since someone has the lock already
            assert False
    except RedLockError:
        # we expect this call to error out
        pass

    lock.release()


def test_fail_to_lock_acquired():
    lock1 = RedLock("test_fail_to_lock_acquired", [{"host": "localhost"}], ttl=1000)
    lock2 = RedLock("test_fail_to_lock_acquired", [{"host": "localhost"}], ttl=1000)

    lock1_locked = lock1.acquire()
    lock2_locked = lock2.acquire()
    lock1.release()

    assert lock1_locked == True
    assert lock2_locked == False


def test_lock_expire():
    lock1 = RedLock("test_lock_expire", [{"host": "localhost"}], ttl=500)
    lock1.acquire()
    time.sleep(1)

    # Now lock1 has expired, we can accquire a lock
    lock2 = RedLock("test_lock_expire", [{"host": "localhost"}], ttl=1000)
    locked = lock2.acquire()
    assert locked == True

    lock1.release()
    lock3 = RedLock("test_lock_expire", [{"host": "localhost"}], ttl=1000)
    locked = lock3.acquire()
    assert locked == False


def test_lock_extend():
    test_lock = RedLock("test_lock_extend", ttl=2000)
    test_lock.acquire()
    time.sleep(1)
    test_lock.extend()
    time.sleep(1.5)
    assert test_lock.check() == True


def test_lock_check():
    test_lock = RedLock("test_lock_check")
    assert test_lock.check() == False
    test_lock.acquire()
    assert test_lock.check() == True
    test_lock.release()
    assert test_lock.check() == False


def test_lock_release_return():
    test_lock = RedLock("test_lock_release_return")
    test_lock.acquire()
    assert test_lock.release() == True


def test_passthrough():
    test_lock = ReentrantRedLock('test_reentrant_passthrough')
    assert test_lock.acquire() == True
    test_lock.release() == True


def test_reentrant():
    test_lock = ReentrantRedLock('test_reentrant')
    assert test_lock.acquire() == True
    assert test_lock.acquire() == True
    assert test_lock.release() == None
    assert test_lock.release() == True


def test_reentrant_n():
    test_lock = ReentrantRedLock('test_reentrant_n')
    for _ in range(10):
        assert test_lock.acquire() == True
    for _ in range(9):
        assert test_lock.release() == None
    assert test_lock.release() == True


def test_reentrant_timeout():
    test_lock = ReentrantRedLock('test_reentrant_timeout', ttl=5000)
    assert test_lock.acquire() == True
    time.sleep(6)
    assert test_lock.acquire() == True
    assert test_lock.release() == True


def test_reentrant_acquire_and_extend():
    test_lock = ReentrantRedLock("test_reentrant_acquire_and_extend", ttl=10000)
    # Acquire the lock normally
    assert test_lock.acquire() == True
    # We can acquire/extend it
    assert test_lock.acquire_and_extend() == True
    # Let the lock time out
    time.sleep(11)

    # We can acquire/extend it off the bat
    assert test_lock.acquire_and_extend() == True
    # Acquire it normally again
    assert test_lock.acquire() == True
    # We can still acquire/extend it
    assert test_lock.acquire_and_extend() == True
    # We should have reentered the lock twice (since it timed out)
    assert test_lock.release() == None
    assert test_lock.release() == None
    # And now we should actually be releasing the lock
    assert test_lock.release() == True


def test_lock_with_multi_backend():
    """
    Test a RedLock can be acquired when at least N/2+1 redis instances are alive.
    Set redis instance with port 6380 down or debug sleep during test.
    """
    lock = RedLock("test_simple_lock", connection_details=[
        {"host": "localhost", "port": 6379, "db": 0, "socket_timeout": 0.2},
        {"host": "localhost", "port": 6379, "db": 1, "socket_timeout": 0.2},
        {"host": "localhost", "port": 6380, "db": 0, "socket_timeout": 0.2}], ttl=1000)
    locked = lock.acquire()
    lock.release()
    assert locked == True
