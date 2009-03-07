package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * A strategy for the persistent storage of leaf tiers.
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
public interface LeafStore<T, F extends Comparable<? super F>, A>
{
    /**
     * Allocate an leaf tier that can store the given capacity of object values.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param capacity
     *            The capacity of the leaf tier.
     * @return The address of the leaf tier storage.
     */
    public A allocate(Stash stash, int capacity);

    /**
     * Load a leaf tier from the persistent storage at the given address. Use
     * the given cooper to create a bucket to store the indexed fields. Use the
     * given extractor to extract the index fields.
     * 
     * @param <B>
     *            The bucket type used to store index fields.
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the leaf tier storage.
     * @param cooper
     *            The cooper to use to create a bucket to store the index
     *            fields.
     * @param extractor
     *            The extractor to use to extract the index fields.
     * @return The leaf tier loaded from storage.
     */
    public <B> LeafTier<B, A> load(Stash stash, A address, Cooper<T, F, B> cooper, Extractor<T, F> extractor);

    /**
     * Write a leaf tier to the persistent storage at the given address. Use the
     * given cooper to extract the the indexed fields from a bucket in the leaf
     * tier. Use the given extractor to extract the index fields if the bucket
     * contains the object value instead of the index fields.
     * 
     * @param <B>
     *            The bucket type used to store index fields.
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the leaf tier storage.
     * @param cooper
     *            The cooper to use to create a bucket to store the index
     *            fields.
     * @param extractor
     *            The extractor to use to extract the index fields.
     * @return The leaf tier loaded from storage.
     */
    public <B> void write(Stash stash, LeafTier<B, A> leaf, Cooper<T, F, B> cooper, Extractor<T, F> extractor);

    /**
     * Free a leaf tier from the persistent storage at the given address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the leaf tier storage.
     */
    public void free(Stash stash, A address);
}