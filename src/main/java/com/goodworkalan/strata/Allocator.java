package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * A strategy for allocating persistent storage for inner and leaf tiers.
 * 
 * @author Alan Gutierrez
 * 
 * @param <B>
 *            The bucket type used to store index fields.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
public interface Allocator<B, A>
{
    /**
     * Allocate persistent storage for the given inner tier that can hold the
     * given capacity of branches. The inner tier itself is given so that the in
     * memory strategy can return a reference to it as the address value.
     * 
     * @param stash
     *            A stash of out of band data.
     * @param inner
     *            The inner tier.
     * @param capacity
     *            The number of branches in the inner tier.
     * @return The address of the persistent storage.
     */
    public A allocate(Stash stash, InnerTier<B, A> inner, int capacity);

    /**
     * Allocate persistent storage for the given inner tier that can hold the
     * number of values given by capacity. The leaf tier itself is given so that
     * the in memory strategy can return a reference to it as the address value.
     * 
     * @param stash
     *            A stash of out of band data.
     * @param inner
     *            The inner tier.
     * @param capacity
     *            The number of values in the leaf tier.
     * @return The address of the persistent storage.
     */
    public A allocate(Stash stash, LeafTier<B, A> leaf, int size);

    /**
     * Return true if the given address is the null value for this allocation
     * strategy.
     * 
     * @param address
     *            A storage address.
     * @return True if the address is null.
     */
    public boolean isNull(A address);
    
    /**
     * Get the null address value for this allocation strategy.
     * 
     * @return The null address value.
     */
    public A getNull();
}