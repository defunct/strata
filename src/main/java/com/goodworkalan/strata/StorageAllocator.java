package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * A strategy for allocating persistent storage for inner and leaf tiers that
 * delegates its calls to an implementation of {@link Storage}.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
class StorageAllocator<T, A> implements Allocator<T, A> {
    /** The strategy for persistent storage for inner and leaf tiers. */
    private final Storage<T, A> storage;

    /**
     * Create a new storage allocator.
     * 
     * @param storage
     *            The strategy for persistent storage for inner and leaf tiers.
     */
    public StorageAllocator(Storage<T, A> storage) {
        this.storage = storage;
    }

    /**
     * Allocate persistent storage for the given inner tier that can hold the
     * given capacity of branches from the underlying persistent storage
     * strategy.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param inner
     *            The inner tier.
     * @param capacity
     *            The number of branches in the inner tier.
     * @return The address of the persistent storage.
     */
    public A allocate(Stash stash, InnerTier<T, A> inner, int capacity) {
        return storage.getInnerStore().allocate(stash, capacity);
    }

    /**
     * Allocate persistent storage for the given inner tier that can hold the
     * number of values given by capacity from the underlying persistent storage
     * strategy.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param leaf
     *            The leaf tier.
     * @param capacity
     *            The number of values in the leaf tier.
     * @return The address of the persistent storage.
     */
    public A allocate(Stash stash, LeafTier<T, A> leaf, int capacity) {
        return storage.getLeafStore().allocate(stash, capacity);
    }
}
