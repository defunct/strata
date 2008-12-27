package com.goodworkalan.strata;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

final class ReadLockExtractor
implements LockExtractor
{
    public Lock getSync(ReadWriteLock readWriteLock)
    {
        return readWriteLock.readLock();
    }

    public boolean isExeclusive()
    {
        return false;
    }
}