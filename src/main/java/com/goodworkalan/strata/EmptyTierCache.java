package com.goodworkalan.strata;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.goodworkalan.stash.Stash;

/**
 * A tier cache for in memory storage applications that merely implements
 * the ability to lock the common structure. This implementation
 * immediately calls the write method of the storage implementation when a
 * page is passed to the {@link NullTierCache#dirty dirty()} method. If
 * auto commit is true, the commit method of the storage strategy is
 * called immediately thereafter.
 * <p>
 * The auto commit property will retain the value set, but it does not
 * actually effect the behavior of storage.
 */
public class EmptyTierCache<B, A>
implements TierWriter<B, A>, AutoCommit
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
    
    public EmptyTierCache()
    {
        this(new ReentrantLock(), true);
    }
    
    /**
     * Create an empty tier cache with 
     */
    protected EmptyTierCache(Lock lock, boolean autoCommit)
    {
        this.lock = lock;
        this.autoCommit = autoCommit;
    }
    
    public void autoCommit(Stash stash)
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

    public void dirty(Stash stash, InnerTier<B, A> inner)
    {
    }

    public void remove(InnerTier<B, A> inner)
    {
    }

    public void dirty(Stash stash, LeafTier<B, A> leaf)
    {
    }

    public void remove(LeafTier<B, A> leaf)
    {
    }
    
    /**
     * A noop implementation of storage synchronization called after an
     * insert or delete of an object from the strata.
     *
     * @param storage The storage strategy.
     * @param txn A storage specific state object.
     */
    public void end(Stash stash)
    {
    }
      
    /**
     * Since the cache is always empty, this method merely calls the
     * commit method of the storage strategy.
     *
     * @param storage The storage strategy.
     * @param txn A storage specific state object.
     */
    public void flush(Stash stash)
    {
        autoCommit(stash);
    }
    
    /**
     * Lock the Strata for exclusive inserts and deletes. This does not
     * prevent other threads from reading the Strata.
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
     * Returns a new empty tier cache built from this prototype empty tier
     * cache. This will be a new empty tier cache that references the same
     * exclusive lock on the Strata.
     *
     * @return A new tier cache based on this prototype instance.
     */
    public TierWriter<B, A> newTierWriter()
    {
        return new EmptyTierCache<B, A>(lock, autoCommit);
    }
}