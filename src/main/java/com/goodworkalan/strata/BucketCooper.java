package com.goodworkalan.strata;

import java.io.Serializable;

import com.goodworkalan.stash.Stash;

// TODO Document.
public class BucketCooper<T, F extends Comparable<? super F>>
implements Cooper<T, F, Bucket<T, F>>, Serializable
{
    // TODO Document.
    private final static long serialVersionUID = 20070402L;

    // TODO Document.
    public Bucket<T, F> newBucket(Stash stash, Extractor<T, F> extractor, T object)
    {
        return new Bucket<T, F>(extractor.extract(stash, object), object);
    }

    // TODO Document.
    public F getFields(Stash stash, Extractor<T, F> extractor, Bucket<T, F> bucket)
    {
        return bucket.getFields();
    }

    // TODO Document.
    public T getObject(Bucket<T, F> bucket)
    {
        return bucket.getObject();
    }
    
    // TODO Document.
    public Cursor<T> wrap(Cursor<Bucket<T, F>> cursor)
    {
        return new BucketCursor<T, F, Bucket<T, F>>(cursor);
    }
}