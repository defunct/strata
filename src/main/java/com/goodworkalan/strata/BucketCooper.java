package com.goodworkalan.strata;

import java.io.Serializable;


public class BucketCooper<T, X>
implements Cooper<T, Bucket<T>, X>, Serializable
{
    private final static long serialVersionUID = 20070402L;

    public Bucket<T> newBucket(X txn, Extractor<T, X> extractor, T object)
    {
        CoreRecord record = new CoreRecord();
        extractor.extract(txn, object, record);
        return new Bucket<T>(record.getFields(), object);
    }

    public Bucket<T> newBucket(Comparable<?>[] fields, T object)
    {
        return new Bucket<T>(fields, object);
    }

    public Comparable<?>[] getFields(X txn, Extractor<T, X> extractor, Bucket<T> bucket)
    {
        return bucket.getFields();
    }

    public T getObject(Bucket<T> bucket)
    {
        return bucket.getObject();
    }
    
    public Cursor<T> wrap(Cursor<Bucket<T>> cursor)
    {
        return new BucketCursor<T, Bucket<T>, X>(cursor);
    }
}