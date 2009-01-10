package com.goodworkalan.strata;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.goodworkalan.favorites.Stash;

/**
 * A tier cache that maintains a map of dirty tiers per query and that
 * prevents other queries from inserting or deleting items until the tier
 * cache is emptied and dirty tiers are persisted.
 * <p>
 * The exclusive strata lock will be called when an insert or delete
 * begins and the map of dirty tiers is empty. At the end of an insert or
 * delete, if the map of dirty tiers is empty, the exclusive strata lock
 * is released. Thus, the empty map of dirty tiers is an indicator that
 * the associated query does not hold the lock on the strata.
 */
public class PerQueryTierCache<B, T, F extends Comparable<F>, A>
extends AbstractTierCache<B, T, F, A>
{
    /**
     * Create a per query tier cache.
     *
     * @param lock An exclusive lock on the Strata.
     * @param max The dirty tier cache size that when reached, will cause
     * the cache to empty and the tiers to be written.
     */
    public PerQueryTierCache(Storage<T, F, A> storage, Cooper<T, F, B> cooper, Extractor<T, F> extractor, int max)
    {
        this(storage, cooper, extractor, new ReentrantLock(), max, true);
    }
    
    private PerQueryTierCache(Storage<T, F, A> storage, Cooper<T, F, B> cooper, Extractor<T, F> extractor, Lock lock, int max, boolean autoCommit)
    {
        super(storage, cooper, extractor,
              lock,
              new Object(),
              max,
              autoCommit);
    }
    
    public void begin()
    {
        if (size() == 0)
        {
            lock();
        }
    }
    
    public void end(Stash stash)
    {
        save(stash, false);
        if (size() == 0)
        {
            unlock();
        }
    }
    
    public TierWriter<B, A> newTierCache()
    {
        return new PerQueryTierCache<B, T, F, A>(getStorage(), cooper, extractor, lock, max, isAutoCommit());
    }
}