package com.goodworkalan.strata;

import java.io.Serializable;

import com.goodworkalan.favorites.Stash;


public final class LookupCooper<T, F extends Comparable<F>>
implements Cooper<T, F, T>, Serializable
{
    private final static long serialVersionUID = 20070402L;

    public T newBucket(Stash stash, Extractor<T, F> extract, T object)
    {
        return object;
    }

    public T newBucket(F fields, T object)
    {
        return object;
    }

    public F getFields(Stash stash, Extractor<T, F> extractor, T object)
    {
        return extractor.extract(stash, object);
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