package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * A strategy for allocating persistent storage for inner and leaf tiers.
 * <p>
 * FIXME Reintroduce storage. Allocator becomes the interface to short circuit
 * persistent storage with in memory strategies. Storage can allocate and free
 * with addresses only. It can load to a collection. It can write a collection.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
public interface Allocator<T, A>
{
    /**
     * Allocate persistent storage for the given inner tier that can hold the
     * given capacity of branches. The inner tier itself is given so that the in
     * memory strategy can return a reference to it as the address value.
     * 
     * @param stash
     *          A type-safe container of out of band data.
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

    /**
     * Load an inner tier from the persistent storage at the given address. Use
     * the given cooper to create a bucket to store the indexed fields. Use the
     * given extractor to extract the index fields.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the inner tier storage.
     * @param inner
     *            The leaf inner to load from storage.
     */
    public void load(Stash stash, A address, InnerTier<T, A> inner);

    /**
     * Load a leaf tier from the persistent storage at the given address. Use
     * the given cooper to create a bucket to store the indexed fields. Use the
     * given extractor to extract the index fields.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the leaf tier storage.
     * @param leaf
     *            The leaf tier to load from storage.
     */
    public void load(Stash stash, A address, LeafTier<T, A> leaf);

    /**
     * Write an inner tier to the persistent storage at the given address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the inner tier storage.
     */
    public void write(Stash stash, InnerTier<T, A> inner);

    /**
     * Write a leaf tier to the persistent storage at the given address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param leaf
     *            The dirty leaf tier.
     * @return The leaf tier loaded from storage.
     */
    public void write(Stash stash, LeafTier<T, A> leaf);
    
    /**
     * Free an inner tier from the persistent storage at the given address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the inner tier storage.
     */
    public void free(Stash stash, InnerTier<T, A> inner);

    /**
     * Free a leaf tier from the persistent storage at the given address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param leaf
     *            The leaf tier.
     */
    public void free(Stash stash, LeafTier<T, A> leaf);

    /**
     * Get the null address value for this allocation strategy.
     * 
     * @return The null address value.
     */
    public A getNull();

    /**
     * Return true if the given address is the null value for this allocation
     * strategy.
     * 
     * @param address
     *            A storage address.
     * @return True if the address is null.
     */
    public boolean isNull(A address);
}