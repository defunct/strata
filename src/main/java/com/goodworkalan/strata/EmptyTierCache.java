package com.goodworkalan.strata;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.goodworkalan.stash.Stash;

/**
 * A tier cache for in memory storage applications that merely implements the
 * ability to lock the common structure. This implementation immediately calls
 * the write method of the storage implementation when a page is passed to the
 * {@link NullTierCache#dirty dirty()} method. If auto commit is true, the
 * commit method of the storage strategy is called immediately thereafter.
 * <p>
 * The auto commit property will retain the value set, but it does not actually
 * effect the behavior of storage.
 */
public class EmptyTierCache<B, A>
implements TierWriter<B, A>
{
    /**
     * A lock instance that will exclusively lock the Strata for insert and
     * delete. This lock instance is common to all tier caches generated
     * by the tier cache prototype.
     */
    protected final Lock lock;
    
    /**
     * A count of the number of times the lock method was called on this
     * tier cache instance.
     */
    protected int lockCount;
    
    /** 
     * If true the tier cache will invoke the commit method of the storage
     * implementation after the tier cache writes a set of dirty tiers.
     */
    private boolean autoCommit;
    
    /**
     * Create an empty tier cache.
     */
    public EmptyTierCache()
    {
        this(new ReentrantLock(), true);
    }

    /**
     * Create an empty tier cache guarded by the given lock with that will auto
     * commit according to the given auto commit flag.
     * 
     * @param lock
     *            A lock that guards the tier writer.
     * @param If
     *            true the tier writer will auto commit after it writes a set of
     *            dirty tiers.
     */
    protected EmptyTierCache(Lock lock, boolean autoCommit)
    {
        this.lock = lock;
        this.autoCommit = autoCommit;
    }

    /**
     * Auto commit by calling the commit method of the storage strategy if the
     * auto commit flag is set.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     */
    protected void autoCommit(Stash stash)
    {
    }
    
    /**
     * Determines if the tier cache will invoke the commit method of the
     * storage implementation after the tier cache writes a set of dirty
     * tiers.
     *
     * @return True if the tier cache will auto commit.
     */
    public boolean isAutoCommit()
    {
        return autoCommit;
    }
    
    /**
     * Sets whether the tier cache will invoke the commit method of the
     * storage implementation after the tier cache writes a set of dirty
     * tiers.
     *
     * @param autoCommit If true the tier cache will auto commit.
     */
    public void setAutoCommit(boolean autoCommit)
    {
        this.autoCommit = autoCommit;
    }
    
    /**
     * Lock the strata exclusive for inserts and deletes. This does not
     * prevent other threads from reading the strata.
     */
    public void lock()
    {
        if (lockCount == 0)
        {
            lock.lock();
        }
        lockCount++;
    }

    /**
     * A noop implementation of storage synchronization called before an
     * insert or delete of an object from the strata.
     */
    public void begin()
    {
    }

    /**
     * Does nothing since this tier cache is always empty.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param inner
     *            The dirty tier.
     */
    public void dirty(Stash stash, InnerTier<B, A> inner)
    {
    }

    /**
     * Does nothing since this tier cache is always empty.
     * 
     * @param inner The tier to remove.
     */
    public void remove(InnerTier<B, A> inner)
    {
    }

    /**
     * Does nothing since this tier cache is always empty.
     * 
     * @param storage
     *            The storage strategy.
     * @param stash
     *            A type-safe container of out of band data.
     * @param leaf
     *            The dirty leaf tier.
     */
    public void dirty(Stash stash, LeafTier<B, A> leaf)
    {
    }

    /**
     * Does nothing since this tier cache is always empty.
     * 
     * @param leaf The leaf tier to remove.
     */
    public void remove(LeafTier<B, A> leaf)
    {
    }

    /**
     * A noop implementation of storage synchronization called after an insert
     * or delete of an object from the strata.
     * 
     * @param storage
     *            The storage strategy.
     * @param stash
     *            A type-safe container of out of band data.
     */
    public void end(Stash stash)
    {
    }

    /**
     * Since the cache is always empty, this method merely calls the commit
     * method of the storage strategy.
     * 
     * @param storage
     *            The storage strategy.
     * @param stash
     *            A type-safe container of out of band data.
     */
    public void flush(Stash stash)
    {
        autoCommit(stash);
    }

    /**
     * Unlock the tree for exclusive inserts and deletes. This does not prevent
     * other threads from reading the tree.
     */
    public void unlock()
    {
        lockCount--;
        if (lockCount == 0)
        {
            lock.unlock();
        }
    }

    /**
     * Returns a new empty tier writer built from this prototype empty tier
     * writer. This will be a new empty tier writer that references the same
     * exclusive lock on the tree.
     * 
     * @return A new tier writer based on this prototype instance.
     */
    public TierWriter<B, A> newTierWriter()
    {
        return new EmptyTierCache<B, A>(lock, autoCommit);
    }
}