/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.io.Serializable;

public class BucketCooper<T, F extends Comparable<? super F>, B, X>
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
        return bucket.fields;
    }

    public T getObject(Bucket<T, F> bucket)
    {
        return bucket.object;
    }

    public boolean getCacheFields()
    {
        return true;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */