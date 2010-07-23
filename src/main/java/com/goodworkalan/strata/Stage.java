package com.goodworkalan.strata;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.goodworkalan.stash.Stash;

/**
 * A dirty page cache that writes dirty pages and frees freed pages at the end
 * of a transaction.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the indexed objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
public class Stage<T, A> {
    // FIXME What does it mean when you do not try/catch?
    /** A read/write lock on insert and delete operations. */
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    /** The allocator to use to load pages from disk. */
    private final Storage<T, A> allocator;

    /** A set of dirty tiers. */
    private final Set<Tier<T, A>> dirtyTiers = new HashSet<Tier<T,A>>();
    
    /** A set of tiers to free. */
    private final Set<Tier<T, A>> freeTiers = new HashSet<Tier<T,A>>();
    
    /** The maximum number of dirty tiers to hold in memory. */
    private final int maxDirtyTiers;

    /**
     * Create a new tier writer.
     * 
     * @param allocator
     *            The allocator to use to load pages from disk.
     * @param maxDirtyTiers
     *            The maximum number of dirty tiers to hold in memory.
     */
    public Stage(Storage<T, A> allocator, int maxDirtyTiers) {
        this.allocator = allocator;
        this.maxDirtyTiers = maxDirtyTiers;
    }

    /**
     * Get the count of staged dirty and freed tiers.
     * 
     * @return The count of staged dirty and free tiers.
     */
    private int getDirtyTierCount() {
        return dirtyTiers.size() + freeTiers.size();
    }

    /**
     * Get the lock that locks the b+tree exclusively for insert and update.
     * 
     * @return The insert delete lock.
     */
    public Lock getInsertDeleteLock() {
        return readWriteLock.writeLock();
    }

    /**
     * Lock the b+tree prior to an insert and delete operation.
     */
    public void begin() {
        getLock().lock();
    }

    /**
     * Lock the b+tree after an insert and delete operation.
     * 
     * @param count
     *            A reference to the count of times that begin locked that end
     *            did not unlock.
     */
    public void end(int[] count) {
        count[0]++;
        if (getDirtyTierCount() == 0) {
            Lock lock = getLock();
            while (count[0]-- != 0) {
                lock.unlock();
            }
        }
    }

    /**
     * Return the lock used to guard the writes during an insert or delete.
     * 
     * @return The lock used to guard the writes.
     */
    private Lock getLock() {
        return maxDirtyTiers == 0 ? readWriteLock.readLock() : readWriteLock.writeLock();
    }

    /**
     * Mark a tier as dirty. The inner tier will be written by the next call to
     * {@link #flush(Stash, int[], boolean) flush}.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param tier
     *            The tier to mark dirty.
     */
    public void dirty(Stash stash, Tier<T, A> tier) {
        dirtyTiers.add(tier);
    }

    /**
     * Free a tier. The inner tier will be freed by the next call to
     * {@link #flush(Stash, int[], boolean) flush}.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param leaf
     *            The tier to free.
     */
    public void free(Stash stash, Tier<T, A> tier) {
        dirtyTiers.remove(tier);
        freeTiers.add(tier);
    }

    /**
     * Write the dirty pages and free the freed pages if this writer holds more
     * tiers than the maximum dirty tiers or if the given force flag is true.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param count
     *            A reference to the count of time that begin locked that end
     *            did not unlock.
     * @param force
     *            Flush the tier writer regardless of its max dirty tiers
     *            property.
     */
    public void flush(Stash stash, int[] count, boolean force) {
        if (maxDirtyTiers != 0) {
            readWriteLock.writeLock().lock();
            if (force || maxDirtyTiers < getDirtyTierCount()) {
                for (Tier<T, A> inner : dirtyTiers) {
                    allocator.write(stash, inner);
                }
                for (Tier<T, A> inner : freeTiers) {
                    allocator.free(stash, inner.getAddress());
                }
                while (count[0]-- != 0) {
                    readWriteLock.writeLock().unlock();
                }
            }
            readWriteLock.writeLock().unlock();
        }
    }
}
