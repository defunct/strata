package com.goodworkalan.strata;

import com.goodworkalan.favorites.Stash;

public interface Cooper<T, F extends Comparable<F>, B>
{
    public B newBucket(Stash stash, Extractor<T, F> extractor, T object);

    public B newBucket(F fields, T object);

    public T getObject(B bucket);

    public F getFields(Stash stash, Extractor<T, F> extractor, B bucket);
    
    public Cursor<T> wrap(Cursor<B> cursor);
}