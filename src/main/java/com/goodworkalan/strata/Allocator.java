package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * A strategy for allocating persistent storage for inner and leaf tiers.
 * <p>
 * This interface is used to chose between the default in-memory storage
 * strategy, which simply returns an object reference to the tier object as the
 * object address, and an actual persistent storage strategy implementation of
 * the {@link Storage} interface.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
interface Allocator<T, A> {
    /**
     * Allocate persistent storage for the given inner tier that can hold the
     * given capacity of branches. The inner tier itself is given so that the in
     * memory strategy can return a reference to it as the address value.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param inner
     *            The inner tier.
     * @param capacity
     *            The number of branches in the inner tier.
     * @return The address of the persistent storage.
     */
    public A allocate(Stash stash, InnerTier<T, A> inner, int capacity);

    /**
     * Allocate persistent storage for the given inner tier that can hold the
     * number of values given by capacity. The leaf tier itself is given so that
     * the in memory strategy can return a reference to it as the address value.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param leaf
     *            The leaf tier.
     * @param capacity
     *            The number of values in the leaf tier.
     * @return The address of the persistent storage.
     */
    public A allocate(Stash stash, LeafTier<T, A> leaf, int capacity);
}
