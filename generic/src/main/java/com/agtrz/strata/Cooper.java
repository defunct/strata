/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public interface Cooper<T, F extends Comparable<? super F>, B, X>
{
    public B newBucket(X txn, Extractor<T, F, X> extract, T object);

    public B newBucket(F fields, T object);

    public T getObject(B bucket);

    public F getFields(X txn, Extractor<T, F, X> extractor, B bucket);

    public boolean getCacheFields();
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */