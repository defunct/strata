package com.goodworkalan.strata;

import java.io.Serializable;


public class BucketCooper<T, F extends Comparable<F>, X>
implements Cooper<T, F, Bucket<T, F>, X>, Serializable
{
    private final static long serialVersionUID = 20070402L;

    public Bucket<T, F> newBucket(X txn, Extractor<T, F, X> extractor, T object)
    {
        return new Bucket<T, F>(extractor.extract(txn, object), object);
    }

    public Bucket<T, F> newBucket(F fields, T object)
    {
        return new Bucket<T, F>(fields, object);
    }

    public F getFields(X txn, Extractor<T, F, X> extractor, Bucket<T, F> bucket)
    {
        return bucket.getFields();
    }

    public T getObject(Bucket<T, F> bucket)
    {
        return bucket.getObject();
    }
    
    public Cursor<T> wrap(Cursor<Bucket<T, F>> cursor)
    {
        return new BucketCursor<T, F, Bucket<T, F>, X>(cursor);
    }
}