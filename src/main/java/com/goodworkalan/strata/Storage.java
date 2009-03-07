package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * A strategy for persistent storage of inner and leaf tiers.
 * 
 * @author Alan Gutierrez
 *
 * @param <T>
 *            The value type of the indexed objects.
 * @param <F>
 *            The field type used to index the objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
public interface Storage<T, F extends Comparable<? super F>, A>
{
    /**
     * Get the strategy for the persistent storage of inner tiers.
     * 
     * @return The strategy for the persistent storage of inner tiers.
     */
    public InnerStore<T, F, A> getInnerStore();
    
    /**
     * Get the strategy for the persistent storage of leaf tiers.
     * 
     * @return The strategy for the persistent storage of leaf tiers.
     */
    public LeafStore<T, F, A> getLeafStore();

    /**
     * Commit the changes written to disk through the inner store and leaf store
     * forcing them to persistent storage.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     */
    public void commit(Stash stash);
    
    /**
     * Get the null address value for this storage strategy.
     * 
     * @return The null address value.
     */
    public A getNull();

    /**
     * Return true if the given address is the null value for this storage
     * strategy.
     * 
     * @param address
     *            A storage address.
     * @return True if the address is null.
     */
    public boolean isNull(A address);
}