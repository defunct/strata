package com.goodworkalan.strata;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * A swapable strategy that extracts either the read or write lock from a
 * read/write lock.
 */
interface LockExtractor {
    /**
     * Get either the read or the write lock from a read/write lock.
     * 
     * @param readWriteLock
     *            The read/write lock.
     * @return Either the read or the write lock from a read/write lock.
     */
    public Lock getLock(ReadWriteLock readWriteLock);

    /**
     * Return true if the lock extractor returns the write lock.
     * 
     * @return True if the lock extractor returns the write lock.
     */
    public boolean isWrite();
}