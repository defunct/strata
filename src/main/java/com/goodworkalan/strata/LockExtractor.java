package com.goodworkalan.strata;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

// TODO Document.
interface LockExtractor
{
    // TODO Document.
    public Lock getSync(ReadWriteLock readWriteLock);

    // TODO Document.
    public boolean isExeclusive();
}