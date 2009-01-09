package com.goodworkalan.strata;

public interface Cooper<T, F extends Comparable<F>, B, X>
{
    public B newBucket(X txn, Extractor<T, F, X> extractor, T object);

    public B newBucket(F fields, T object);

    public T getObject(B bucket);

    public F getFields(X txn, Extractor<T, F, X> extractor, B bucket);
    
    public Cursor<T> wrap(Cursor<B> cursor);
}