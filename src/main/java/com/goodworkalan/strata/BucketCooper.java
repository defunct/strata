package com.goodworkalan.strata;

import java.io.Serializable;

import com.goodworkalan.favorites.Stash;


public class BucketCooper<T, F extends Comparable<F>>
implements Cooper<T, F, Bucket<T, F>>, Serializable
{
    private final static long serialVersionUID = 20070402L;

    public Bucket<T, F> newBucket(Stash stash, Extractor<T, F> extractor, T object)
    {
        return new Bucket<T, F>(extractor.extract(stash, object), object);
    }

    public Bucket<T, F> newBucket(F fields, T object)
    {
        return new Bucket<T, F>(fields, object);
    }

    public F getFields(Stash stash, Extractor<T, F> extractor, Bucket<T, F> bucket)
    {
        return bucket.getFields();
    }

    public T getObject(Bucket<T, F> bucket)
    {
        return bucket.getObject();
    }
    
    public Cursor<T> wrap(Cursor<Bucket<T, F>> cursor)
    {
        return new BucketCursor<T, F, Bucket<T, F>>(cursor);
    }
}