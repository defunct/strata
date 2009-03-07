package com.goodworkalan.strata;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import com.goodworkalan.stash.Stash;

/**
 * Keeps a synchronized map of dirty tiers with a maximum size at which the
 * tiers are written to file and flushed. Used as the base class of both the per
 * query and per tree implementations of the tier cache.
 * 
 * @param <B>
 *            The bucket type used to store index fields.
 * @param <T>
 *            The value type of the indexed objects.
 * @param <F>
 *            The field type used to index the objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 * @author Alan Gutierrez
 * FIXME Rename AbstractTierWriter.
 */
class AbstractTierCache<B, T, F extends Comparable<? super F>, A>
extends EmptyTierCache<B, A>
{
    /** The map of addresses to leaf tiers. */
    private final Map<A, LeafTier<B, A>> leafTiers;
    
    /** The map of addresses to inner tiers. */
    private final Map<A, InnerTier<B, A>> innerTiers;
    
    /** The strategy for persistent storage of inner and leaf tiers. */
    private final Storage<T, F, A> storage;
    
    /** The cooper to use to create a bucket to store the index fields. */
    protected final Cooper<T, F, B> cooper;
    
    /** The extractor to use to extract the index fields. */
    protected final Extractor<T, F> extractor;

    /** A monitor used to guard the maps of inner and leaf tiers. */
    protected final Object monitor;
    
    /**
     * The dirty tier cache size that when reached, will cause the cache to
     * empty and the tiers to be written.
     */
    protected final int max;

    /**
     * Create a copy of this tier writer.
     * 
     * @param storage
     *            The persistent storage strategy.
     * @param cooper
     *            The cooper to use to create a bucket to store the index
     *            fields.
     * @param extractor
     *            The extractor to use to extract the index fields.
     * @param lock
     *            A lock instance that will exclusively lock the tree for insert
     *            and delete.
     * @param monitor
     *            A monitor used to guard the maps of inner and leaf tiers.
     * @param max
     *            The dirty tier cache size that when reached, will cause the
     *            cache to empty and the tiers to be written.
     * @param autoCommit
     *            If true, the commit method of the storage strategy is called
     *            after the dirty tiers are written.
     */
    protected AbstractTierCache(Storage<T, F, A> storage,
            Cooper<T, F, B> cooper, Extractor<T, F> extractor, Lock lock,
            Object monitor, int max, boolean autoCommit)
    {
        super(lock, autoCommit);
        this.storage = storage;
        this.cooper = cooper;
        this.extractor = extractor;
        this.monitor = monitor;
        this.max = max;
        this.leafTiers = new HashMap<A, LeafTier<B,A>>();
        this.innerTiers = new HashMap<A, InnerTier<B,A>>();
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
        if (isAutoCommit())
        {
            storage.commit(stash);
        }
    }

    /**
     * Get the persistent storage strategy.
     * 
     * @return The persistent storage strategy.
     */
    protected Storage<T, F, A> getStorage()
    {
        return storage;
    }

    /**
     * Get the count of leaf and inner tiers cached by the tier writer.
     * 
     * @return The count of leaf and inner tiers.
     */
    public int size()
    {
        return innerTiers.size() + leafTiers.size();
    }

    /**
     * Empty the dirty tier cache by writing out the dirty tiers and clearing
     * the map of dirty tiers. If force is true, we do not check that the
     * maximum size has been reached. If the auto commit is true, then the
     * commit method of the storage strategy is called.
     * 
     * @param storage
     *            The storage strategy.
     * @param txn
     *            A storage specific state object.
     * @param force
     *            If true save unconditionally, do not check the maximum size.
     */
    protected void save(Stash stash, boolean force)
    {
        synchronized (monitor)
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
    
    /**
     * Record the given inner tier as dirty in the tier cache.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param inner
     *            The dirty tier.
     */
    @Override
    public void dirty(Stash stash, InnerTier<B,A> inner)
    {
        synchronized (monitor)
        {
            innerTiers.put(inner.getAddress(), inner);
        }
    }
    
    /**
     * Remove the given dirty inner tier from the tier cache.
     * 
     * @param inner The tier to remove.
     */
    @Override
    public void remove(InnerTier<B, A> inner)
    {
        synchronized (monitor)
        {
            innerTiers.remove(inner);
        }
    }
    
    /**
     * Record the given leaf tier as dirty in the tier cache.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param leaf
     *            The dirty leaf tier.
     */
    @Override
    public void dirty(Stash stash, LeafTier<B,A> leaf)
    {
        synchronized (monitor)
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