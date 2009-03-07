package com.goodworkalan.strata;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

// TODO Document.
final class WriteLockExtractor
implements LockExtractor
{
    // TODO Document.
    public Lock getSync(ReadWriteLock readWriteLock)
    {
        return readWriteLock.writeLock();
    }

    // TODO Document.
    public boolean isExeclusive()
    {
        return true;
    }
}