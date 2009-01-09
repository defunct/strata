package com.goodworkalan.strata;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;

/**
 * Keeps a synchronized map of dirty tiers with a maximum size at which
 * the tiers are written to file and flushed. Used as the base class of
 * both the per query and per strata implementations of the tier cache.
 */
class AbstractTierCache<B, T, F extends Comparable<F>, A, X>
extends EmptyTierCache<B, A, X>
{
    private final Map<A, LeafTier<B, A>> leafTiers;
    
    private final Map<A, InnerTier<B, A>> innerTiers;
    
    private final Storage<T, F, A, X> storage;
    
    protected final Cooper<T, F, B, X> cooper;
    
    protected final Extractor<T, F, X> extractor;

    protected final Object mutex;
    
    /**
     * The dirty tier cache size that when reached, will cause the cache
     * to empty and the tiers to be written.
     */
    protected final int max;

    /**
     * Create a tier cache using the specified map of dirty tiers and the
     * that flushes when the maximum size is reached. The lock is an
     * exclusive lock on the strata.
     *
     * @param lock An exclusive lock on the Strata.
     * @param mapOfDirtyTiers The map of dirty tiers.
     * @param max The dirty tier cache size that when reached, will cause
     * the cache to empty and the tiers to be written.
     * @param autoCommit If true, the commit method of the storage
     * strategy is called after the dirty tiers are written.
     */
    public AbstractTierCache(Storage<T, F, A, X> storage,
                             Cooper<T, F, B, X> cooper,
                             Extractor<T, F, X> extractor,
                             Lock lock,
                             Object mutex,
                             int max,
                             boolean autoCommit)
    {
        super(lock, autoCommit);
        this.storage = storage;
        this.cooper = cooper;
        this.extractor = extractor;
        this.mutex = mutex;
        this.max = max;
        this.leafTiers = new HashMap<A, LeafTier<B,A>>();
        this.innerTiers = new HashMap<A, InnerTier<B,A>>();
    }

    public void autoCommit(X txn)
    {
        if (isAutoCommit())
        {
            storage.commit(txn);
        }
    }
    
    public Storage<T, F, A, X> getStorage()
    {
        return storage;
    }
    
    public int size()
    {
        return innerTiers.size() + leafTiers.size();
    }

    /**
     * Empty the dirty tier cache by writing out the dirty tiers and
     * clearing the map of dirty tiers. If force is true, we do not
     * check that the maximum size has been reached.  If the auto commit
     * is true, then the commit method of the storage strategy is called.
     *
     * @param storage The storage strategy.
     * @param txn A storage specific state object.
     * @param force If true save unconditionally, do not check the
     * maximum size.
     */
    protected void save(X txn, boolean force)
    {
        synchronized (mutex)
        {
            if (force || innerTiers.size() + leafTiers.size() >= max)
            {
                for (InnerTier<B, A> inner : innerTiers.values())
                {
                    storage.getInnerStore().write(txn, inner, cooper, extractor);
                }
                innerTiers.clear();
                for (LeafTier<B, A> leaf : leafTiers.values())
                {
                    storage.getLeafStore().write(txn, leaf, cooper, extractor);
                }
                leafTiers.clear();
                if (isAutoCommit())
                {
                    getStorage().commit(txn);
                }
            }
        }
    }
    
    @Override
    public void dirty(X txn, InnerTier<B,A> inner)
    {
        synchronized (mutex)
        {
            innerTiers.put(inner.getAddress(), inner);
        }
    }
    
    @Override
    public void remove(InnerTier<B, A> inner)
    {
        synchronized (mutex)
        {
            innerTiers.remove(inner);
        }
    }
    
    @Override
    public void dirty(X txn, LeafTier<B,A> leaf)
    {
        synchronized (mutex)
        {
            leafTiers.put(leaf.getAddress(), leaf);
        }
    }
    
    
    /**
     * Empty the dirty tier cache by writing out the dirty tiers and
     * clearing the map of dirty tiers.
     *
     * @param storage The storage strategy.
     * @param txn A storage specific state object.
     */
    @Override
    public void flush(X txn)
    {
        save(txn, true);
    }
}