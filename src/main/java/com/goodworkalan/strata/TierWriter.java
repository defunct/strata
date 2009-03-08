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
public class TierWriter<T, A>
{
    /** A read/write lock on insert and delete operations. */
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    
    /** The allocator to use to load pages from disk. */
    private final Allocator<T, A> allocator;

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
    public TierWriter(Allocator<T, A> allocator, int maxDirtyTiers)
    {
        this.allocator = allocator;
        this.maxDirtyTiers = maxDirtyTiers;
    }
    
    /**
     * Allow the user to lock the b+tree exclusively for insert and delete.
     */
    public void lock()
    {
        readWriteLock.writeLock().lock();
    }
    
    /**
     * Allow the user to unlock the b+tree exclusively for insert and delete.
     */
    public void unlock()
    {
        readWriteLock.writeLock().unlock();
    }
    
    /**
     * Lock the b+tree prior to an insert and delete operation. 
     */
    public void begin()
    {
        Lock lock = maxDirtyTiers == 0 ? readWriteLock.readLock() : readWriteLock.writeLock();
        lock.lock();
    }
    
    /**
     * Lock the b+tree after an insert and delete operation. 
     */
    public void end()
    {
        Lock lock = maxDirtyTiers == 0 ? readWriteLock.readLock() : readWriteLock.writeLock();
        lock.unlock();
    }

    /**
     * Mark a inner tier as dirty. If the writer has a max dirty tiers of zero,
     * then the inner tier is written immediately. Otherwise, the inner tier
     * will be written by the next call to {@link #flush(Stash, boolean) flush}.
     * 
     * @param leaf
     *            The leaf to free.
     */
    public void dirty(Stash stash, InnerTier<T, A> inner) 
    {
        if (maxDirtyTiers == 0)
        {
            allocator.write(stash, inner);
        }
        else
        {
            dirtyInnerTiers.add(inner);
        }
    }

    /**
     * Mark a leaf tier as dirty. If the writer has a max dirty tiers of zero,
     * then the leaf tier is written immediately. Otherwise, the leaf tier will
     * be written by the next call to {@link #flush(Stash, boolean) flush}.
     * 
     * @param leaf
     *            The leaf to free.
     */
    public void dirty(Stash stash, LeafTier<T, A> leaf)
    {
        if (maxDirtyTiers == 0)
        {
            allocator.write(stash, leaf);
        }
        else
        {
            dirtyLeafTiers.add(leaf);
        }
    }

    /**
     * Free an inner tier. If the writer has a max dirty tiers of zero, then the
     * inner tier is freed immediately. Otherwise, the inner tier will be freed
     * by the next call to {@link #flush(Stash, boolean) flush}.
     * 
     * @param leaf
     *            The leaf to free.
     */
    public void free(Stash stash, InnerTier<T, A> inner)
    {
        if (maxDirtyTiers == 0)
        {
            allocator.free(stash, inner);
        }
        else
        {
            dirtyInnerTiers.remove(inner);
            freeInnerTiers.add(inner);
        }
    }

    /**
     * Free an leaf tier. If the writer has a max dirty tiers of zero, then the
     * leaf tier is freed immediately. Otherwise, the leaf tier will be freed by
     * the next call to {@link #flush(Stash, boolean) flush}.
     * 
     * @param leaf
     *            The leaf to free.
     */
    public void free(Stash stash, LeafTier<T, A> leaf)
    {
        if (maxDirtyTiers == 0)
        {
            allocator.free(stash, leaf);
        }
        else
        {
            dirtyLeafTiers.remove(leaf);
            freeLeafTiers.add(leaf);
        }
    }

    /**
     * Write the dirty pages and free the freed pages if this writer holds more
     * tiers than the maximum dirty tiers or if the given force flag is true.
     * 
     * @param stash
     * @param force Flush the tier writer regardless of its max dirty tiers property.
     */
    public void flush(Stash stash, boolean force)
    {
        if (force || maxDirtyTiers < dirtyInnerTiers.size() + dirtyLeafTiers.size() + freeInnerTiers.size() + freeLeafTiers.size())
        {
            for (InnerTier<T, A> inner : dirtyInnerTiers)
            {
                allocator.write(stash, inner);
            }
            for (InnerTier<T, A> inner : freeInnerTiers)
            {
                allocator.free(stash, inner);
            }
            for (LeafTier<T, A> leaf : dirtyLeafTiers)
            {
                allocator.write(stash, leaf);
            }
            for (LeafTier<T, A> leaf : freeLeafTiers)
            {
                allocator.free(stash, leaf);
            }
        }
    }
}
