package com.goodworkalan.strata;

import java.io.Serializable;

import com.goodworkalan.stash.Stash;

/**
 * A strategy for storing object values in inner and leaf tiers that caches the
 * index fields in a bucket containing the index fields and the object value.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the indexed objects.
 * @param <F>
 *            The field type used to index the objects.
 */
public class BucketCooper<T, F extends Comparable<? super F>>
implements Cooper<T, F, Bucket<T, F>>, Serializable
{
    /** The serial version id. */
    private final static long serialVersionUID = 20070402L;

    /**
     * Create a bucket for the given object extracting the index fields using
     * the given extractor. The given stash is used to provide additional
     * participants in the storage solution.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param extractor
     *            The extractor to use to extract the index fields.
     * @param object
     *            The object to index.
     * @return A bucket that references the fields used to order the tree.
     */
    public Bucket<T, F> newBucket(Stash stash, Extractor<T, F> extractor, T object)
    {
        return new Bucket<T, F>(extractor.extract(stash, object), object);
    }

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
    public F getFields(Stash stash, Extractor<T, F> extractor, Bucket<T, F> bucket)
    {
        return bucket.getFields();
    }

    /**
     * Get the object value from the given bucket.
     * 
     * @param bucket
     *            The bucket.
     * @return The object value.
     */
    public T getObject(Bucket<T, F> bucket)
    {
        return bucket.getObject();
    }
    
    /**
     * Wrap a bucket cursor with a cursor that will extract the object value
     * from the buckets returned by the cursor.
     * 
     * @param cursor
     *            The bucket cursor.
     * @return An object value cursor.
     */
    public Cursor<T> wrap(Cursor<Bucket<T, F>> cursor)
    {
        return new BucketCursor<T, F, Bucket<T, F>>(cursor);
    }
}