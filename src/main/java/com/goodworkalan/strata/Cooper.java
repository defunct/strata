package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * A factory interface for creating the buckets that cache the fields used to
 * order the tree in the inner and leaf tiers of the tree.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The type of object ordered in the tree.
 * @param <F>
 *            The fields used to order the tree.
 * @param <B>
 *            The object used to reference the fields used to order the tree.
 */
public interface Cooper<T, F extends Comparable<? super F>, B>
{
    /**
     * Create a bucket for the given <code>object</code> extracting the index
     * fields using the given <code>extractor</code>. The given
     * <code>stash</code> is used to provide additional participants in the
     * storage solution.
     * 
     * @param stash
     *             A type-safe container of out of band data.
     * @param extractor
     *            The extractor to use to extract the index fields.
     * @param object
     *            The object to index.
     * @return A bucket that references the fields used to order the tree.
     */
    public B newBucket(Stash stash, Extractor<T, F> extractor, T object);

    /**
     * Get the object value from the given bucket.
     * 
     * @param bucket
     *            The bucket.
     * @return The object value.
     */
    public T getObject(B bucket);

    /**
     * Get the index fields from the given bucket.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param extractor
     *            The extractor to use to extract the index fields if necessary.
     * @param bucket
     *            The bucket.
     * @return The index fields.
     */
    public F getFields(Stash stash, Extractor<T, F> extractor, B bucket);

    /**
     * Wrap a bucket cursor with a cursor that will extract the object value
     * from the buckets returned by the cursor.
     * 
     * @param cursor
     *            The bucket cursor.
     * @return An object value cursor.
     */
    public Cursor<T> wrap(Cursor<B> cursor);
}