package com.goodworkalan.strata;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

// TODO Document.
final class ReadLockExtractor
implements LockExtractor
{
    // TODO Document.
    public Lock getLock(ReadWriteLock readWriteLock)
    {
        return readWriteLock.readLock();
    }

    // TODO Document.
    public boolean isWrite()
    {
        return false;
    }
}