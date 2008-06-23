/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.io.Serializable;

public final class LookupCooper<T, F extends Comparable<? super F>, X>
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

    public boolean getCacheFields()
    {
        return false;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */