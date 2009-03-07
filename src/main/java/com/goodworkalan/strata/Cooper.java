package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * A factory interface for creating the buckets that reference the fields used
 * to order the tree in the inner and leaf tiers of the tree.
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
     *            Additional participants in the storage solution.
     * @param extractor
     *            The extractor to use to extract the index fields.
     * @param object
     *            The object to index.
     * @return A bucket that references the fields used to order the tree.
     */
    public B newBucket(Stash stash, Extractor<T, F> extractor, T object);

    // TODO Document.
    public T getObject(B bucket);

    // TODO Document.
    public F getFields(Stash stash, Extractor<T, F> extractor, B bucket);
    
    // TODO Document.
    public Cursor<T> wrap(Cursor<B> cursor);
}