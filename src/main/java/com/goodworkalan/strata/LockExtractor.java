package com.goodworkalan.strata;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

interface LockExtractor
{
    public Lock getSync(ReadWriteLock readWriteLock);

    public boolean isExeclusive();
}