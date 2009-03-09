package com.goodworkalan.strata;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * An implementation of lock extractor that extracts the write lock from a
 * read/write lock.
 * 
 * @author Alan Gutierrez
 */
final class WriteLockExtractor
implements LockExtractor
{
    /**
     * Get the write lock from the read/write lock.
     * 
     * @return The write lock.
     */
    public Lock getLock(ReadWriteLock readWriteLock)
    {
        return readWriteLock.writeLock();
    }

    /**
     * Return true since this lock extractor returns the write lock.
     * 
     *  @return True since this is a write lock extractor.
     */
    public boolean isWrite()
    {
        return true;
    }
}