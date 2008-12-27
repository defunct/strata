package com.goodworkalan.strata;

import java.io.Serializable;


public final class LookupCooper<T, X>
implements Cooper<T, T, X>, Serializable
{
    private final static long serialVersionUID = 20070402L;

    public T newBucket(X txn, Extractor<T, X> extract, T object)
    {
        return object;
    }

    public T newBucket(Comparable<?>[] fields, T object)
    {
        return object;
    }

    public Comparable<?>[] getFields(X txn, Extractor<T, X> extractor, T object)
    {
        CoreRecord record = new CoreRecord();
        extractor.extract(txn, object, record);
        return record.getFields();
    }

    public T getObject(T object)
    {
        return object;
    }
    
    public Cursor<T> wrap(Cursor<T> cursor)
    {
        return cursor;
    }
}