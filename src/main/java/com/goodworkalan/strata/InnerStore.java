package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * A strategy for the persistent storage of inner tiers.
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
public interface InnerStore<T, F extends Comparable<? super F>, A>
{
    /**
     * Allocate an inner tier that can store the given capacity of branches.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param capacity
     *            The capacity of the inner tier.
     * @return The address of the inner tier storage.
     */
    public A allocate(Stash stash, int capacity);

    /**
     * Load an inner tier from the persistent storage at the given address. Use
     * the given cooper to create a bucket to store the indexed fields. Use the
     * given extractor to extract the index fields.
     * 
     * @param <B>
     *            The bucket type used to store index fields.
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the inner tier storage.
     * @param cooper
     *            The cooper to use to create a bucket to store the index
     *            fields.
     * @param extractor
     *            The extractor to use to extract the index fields.
     * @return The inner tier loaded from storage.
     */
    public <B> InnerTier<B, A> load(Stash stash, A address, Cooper<T, F, B> cooper, Extractor<T, F> extractor);

    /**
     * Write an inner tier to the persistent storage at the given address. Use
     * the given cooper to extract the the indexed fields from a bucket in a
     * branch of the inner tier. Use the given extractor to extract the index
     * fields if the bucket contains the object value instead of the index
     * fields.
     * 
     * @param <B>
     *            The bucket type used to store index fields.
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the inner tier storage.
     * @param cooper
     *            The cooper to use to create a bucket to store the index
     *            fields.
     * @param extractor
     *            The extractor to use to extract the index fields.
     * @return The inner tier loaded from storage.
     */
    public <B> void write(Stash stash, InnerTier<B, A> inner, Cooper<T, F, B> cooper, Extractor<T, F> extractor);

    /**
     * Free an inner tier from the persistent storage at the given address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the inner tier storage.
     */
    public void free(Stash stash, A address);
}