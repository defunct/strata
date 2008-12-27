package com.goodworkalan.strata;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

final class WriteLockExtractor
implements LockExtractor
{
    public Lock getSync(ReadWriteLock readWriteLock)
    {
        return readWriteLock.writeLock();
    }

    public boolean isExeclusive()
    {
        return true;
    }
}