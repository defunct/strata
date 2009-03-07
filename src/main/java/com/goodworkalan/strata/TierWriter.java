package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * A strategy for both caching dirty tiers in order to writing them out to
 * storage in a batch as well as for locking the tree for exclusive insert and
 * delete.
 */
public interface TierWriter<B, A>
{
    /**
     * Determines if the tier cache will invoke the commit method of the storage
     * implementation after the tier cache writes a set of dirty tiers.
     * 
     * @return True if the tier cache will auto commit.
     */
    public boolean isAutoCommit();

    /**
     * Sets whether the tier cache will invoke the commit method of the storage
     * implementation after the tier cache writes a set of dirty tiers.
     * 
     * @param autoCommit
     *            If true the tier cache will auto commit.
     */
    public void setAutoCommit(boolean autoCommit);

    /**
     * Lock the Strata exclusive for inserts and deletes. This does not prevent
     * other threads from reading the Strata.
     */
    public void lock();

    /**
     * Notify the tier writer that an insert or delete is about to begin so that
     * the tier cache can acquire locks if necessary.
     */
    public void begin();

    /**
     * Record the given inner tier as dirty in the tier cache.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param inner
     *            The dirty tier.
     */
    public void dirty(Stash stash, InnerTier<B, A> inner);
    
    /**
     * Remove the given dirty inner tier from the tier cache.
     * 
     * @param inner The tier to remove.
     */
    public void remove(InnerTier<B, A> inner);

    /**
     * Record the given leaf tier as dirty in the tier cache.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param leaf
     *            The dirty leaf tier.
     */
    public void dirty(Stash stash, LeafTier<B, A> leaf);
    
    /**
     * Remove the given dirty leaf tier from the tier cache.
     * 
     * @param leaf The leaf tier to remove.
     */
    public void remove(LeafTier<B, A> leaf);

    /**
     * Notify the tier cache that an insert or delete has completed and so that
     * the tier cache can determine if the cache should be flushed. If the tier
     * cache is flushed and the auto commit property is true, the tier cache
     * will call the commit method of the storage strategy.
     * 
     * @param storage
     *            The storage strategy.
     * @param stash
     *            A type-safe container of out of band data.
     */
    public void end(Stash stash);

    /**
     * Flush any dirty pages in the tier cache and empty the tier cache. If the
     * auto commit property is true, the tier cache will call the commit method
     * of the storage strategy.
     * 
     * @param storage
     *            The storage strategy.
     * @param stash
     *            A type-safe container of out of band data.
     */
    public void flush(Stash stash);

    /**
     * Lock the tree for exclusive inserts and deletes. This does not prevent
     * other threads from reading the tree.
     */
    public void unlock();

    /**
     * Create a new tier writer based on this prototype tier writer instance.
     * This is part of a prototype construction pattern.
     * 
     * @return A new tier cache based on this prototype instance.
     * <p>
     * FIXME Dead code.
     */
    public TierWriter<B, A> newTierWriter();
}