package com.goodworkalan.strata;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.goodworkalan.stash.Stash;

//TODO Document.
public class PerStrataTierWriter<B, T, F extends Comparable<? super F>, A>
extends AbstractTierCache<B, T, F, A>
{
    /** A read write lock that guards the tree. */
    private final ReadWriteLock readWriteLock;

    // TODO Document.
    public PerStrataTierWriter(Storage<T, F, A> storage, Cooper<T, F, B> cooper, Extractor<T, F> extractor, int max)
    {
        this(storage, cooper, extractor, new ReentrantReadWriteLock(), new Object(), max, true);
    }

    /**
     * Create a copy of this per tree tier writer.
     * 
     * @param storage
     *            The persistent storage strategy.
     * @param cooper
     *            The cooper to use to create a bucket to store the index
     *            fields.
     * @param extractor
     *            The extractor to use to extract the index fields.
     * @param lock
     *            An exclusive lock to guard inserts and deletes on the tree.
     * @param max
     *            The dirty tier cache size that when reached, will cause the
     *            cache to empty and the tiers to be written.
     * @param autoCommit
     *            If true, the commit method of the storage strategy is called
     *            after the dirty tiers are written.
     */
    private PerStrataTierWriter(Storage<T, F, A> storage, Cooper<T, F, B> cooper, Extractor<T, F> extractor,
            ReadWriteLock readWriteLock, Object mutex, int max, boolean autoCommit)
    {
        super(storage, cooper, extractor, readWriteLock.writeLock(), mutex, max, autoCommit);
        this.readWriteLock = readWriteLock;
    }
    
    // TODO Document.
    public void begin()
    {
        if (lockCount == 0)
        {
            readWriteLock.readLock().lock();
        }
    }
    
    // TODO Document.
    public void end(Stash stash)
    {
        save(stash, false);
        if (lockCount == 0)
        {
            readWriteLock.readLock().unlock();
        }
    }
    
    /**
     * Returns a new per query tier writer built from this prototype per query
     * tier writer. This will be a per query empty tier writer with the same max
     * and auto commit properites, that references the same storage, cooper,
     * executor, and exclusive lock on the tree.
     * 
     * @return A new tier writer based on this prototype instance.
     */
    public TierWriter<B, A> newTierCache()
    {
        return new PerStrataTierWriter<B, T, F, A>(getStorage(), cooper, extractor, readWriteLock, monitor, max, isAutoCommit());
    }
}