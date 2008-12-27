package com.goodworkalan.strata;


public interface Cooper<T, B, X>
{
    public B newBucket(X txn, Extractor<T, X> extractor, T object);

    public B newBucket(Comparable<?>[] fields, T object);

    public T getObject(B bucket);

    public Comparable<?>[] getFields(X txn, Extractor<T, X> extractor, B bucket);
    
    public Cursor<T> wrap(Cursor<B> cursor);
}