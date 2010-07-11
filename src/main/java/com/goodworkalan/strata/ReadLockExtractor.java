package com.goodworkalan.strata;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * An implementation of lock extractor that extracts the read lock from a
 * read/write lock.
 * 
 * @author Alan Gutierrez
 */
final class ReadLockExtractor implements LockExtractor {
    /**
     * Get the read lock from the read/write lock.
     * 
     * @return The read lock.
     */
    public Lock getLock(ReadWriteLock readWriteLock) {
        return readWriteLock.readLock();
    }

    /**
     * Return false since this lock extractor returns the read lock.
     * 
     * @return False since this is a read lock extractor.
     */
    public boolean isWrite() {
        return false;
    }
}