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

    /** A set of dirty inner tiers. */
    private final Set<InnerTier<T, A>> dirtyInnerTiers = new HashSet<InnerTier<T,A>>();
    
    /** A set of inner tiers to free. */
    private final Set<InnerTier<T, A>> freeInnerTiers = new HashSet<InnerTier<T,A>>();
    
    /** A set of dirty leaf tiers. */
    private final Set<LeafTier<T, A>> dirtyLeafTiers = new HashSet<LeafTier<T,A>>();
    
    /** A set of leaf tiers to free. */
    private final Set<LeafTier<T, A>> freeLeafTiers = new HashSet<LeafTier<T,A>>();
    
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
        return dirtyInnerTiers.size() + dirtyLeafTiers.size() + freeInnerTiers.size() + freeLeafTiers.size();
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
     * Mark a inner tier as dirty. The inner tier will be written by the next
     * call to {@link #flush(Stash, int[], boolean) flush}.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param leaf
     *            The leaf to free.
     */
    public void dirty(Stash stash, InnerTier<T, A> inner) {
        dirtyInnerTiers.add(inner);
    }

    /**
     * Mark a leaf tier as dirty. The leaf tier will be written by the next call
     * to {@link #flush(Stash, int[], boolean) flush}.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param leaf
     *            The leaf to free.
     */
    public void dirty(Stash stash, LeafTier<T, A> leaf) {
        dirtyLeafTiers.add(leaf);
    }

    /**
     * Free an inner tier. The inner tier will be freed by the next call to
     * {@link #flush(Stash, int[], boolean) flush}.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param leaf
     *            The leaf to free.
     */
    public void free(Stash stash, InnerTier<T, A> inner) {
        dirtyInnerTiers.remove(inner);
        freeInnerTiers.add(inner);
    }

    /**
     * Free an leaf tier. The leaf tier will be freed by the next call to
     * {@link #flush(Stash, int[], boolean) flush}.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param leaf
     *            The leaf to free.
     */
    public void free(Stash stash, LeafTier<T, A> leaf) {
        dirtyLeafTiers.remove(leaf);
        freeLeafTiers.add(leaf);
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
                for (InnerTier<T, A> inner : dirtyInnerTiers) {
                    allocator.getInnerStore().write(stash, inner.getAddress(), inner, inner.getChildType());
                }
                for (InnerTier<T, A> inner : freeInnerTiers) {
                    allocator.getInnerStore().free(stash, inner.getAddress());
                }
                for (LeafTier<T, A> leaf : dirtyLeafTiers) {
                    allocator.getLeafStore().write(stash, leaf.getAddress(), leaf, leaf.getNext());
                }
                for (LeafTier<T, A> leaf : freeLeafTiers) {
                    allocator.getLeafStore().free(stash, leaf.getAddress());
                }
                while (count[0]-- != 0) {
                    readWriteLock.writeLock().unlock();
                }
            }
            readWriteLock.writeLock().unlock();
        }
    }
}
