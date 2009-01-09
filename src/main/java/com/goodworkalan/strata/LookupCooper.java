package com.goodworkalan.strata;

import java.io.Serializable;


public final class LookupCooper<T, F extends Comparable<F>, X>
implements Cooper<T, F, T, X>, Serializable
{
    private final static long serialVersionUID = 20070402L;

    public T newBucket(X txn, Extractor<T, F, X> extract, T object)
    {
        return object;
    }

    public T newBucket(F fields, T object)
    {
        return object;
    }

    public F getFields(X txn, Extractor<T, F, X> extractor, T object)
    {
        return extractor.extract(txn, object);
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