package com.goodworkalan.strata;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import com.goodworkalan.stash.Stash;

/**
 * Keeps a synchronized map of dirty tiers with a maximum size at which
 * the tiers are written to file and flushed. Used as the base class of
 * both the per query and per strata implementations of the tier cache.
 */
class AbstractTierCache<B, T, F extends Comparable<F>, A>
extends EmptyTierCache<B, A>
{
    private final Map<A, LeafTier<B, A>> leafTiers;
    
    private final Map<A, InnerTier<B, A>> innerTiers;
    
    private final Storage<T, F, A> storage;
    
    protected final Cooper<T, F, B> cooper;
    
    protected final Extractor<T, F> extractor;

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
    public AbstractTierCache(Storage<T, F, A> storage,
                             Cooper<T, F, B> cooper,
                             Extractor<T, F> extractor,
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

    public void autoCommit(Stash stash)
    {
        if (isAutoCommit())
        {
            storage.commit(stash);
        }
    }
    
    public Storage<T, F, A> getStorage()
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
    protected void save(Stash stash, boolean force)
    {
        synchronized (mutex)
        {
            if (force || innerTiers.size() + leafTiers.size() >= max)
            {
                for (InnerTier<B, A> inner : innerTiers.values())
                {
                    storage.getInnerStore().write(stash, inner, cooper, extractor);
                }
                innerTiers.clear();
                for (LeafTier<B, A> leaf : leafTiers.values())
                {
                    storage.getLeafStore().write(stash, leaf, cooper, extractor);
                }
                leafTiers.clear();
                if (isAutoCommit())
                {
                    getStorage().commit(stash);
                }
            }
        }
    }
    
    @Override
    public void dirty(Stash stash, InnerTier<B,A> inner)
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
    public void dirty(Stash stash, LeafTier<B,A> leaf)
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
    public void flush(Stash stash)
    {
        save(stash, true);
    }
}