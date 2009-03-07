package com.goodworkalan.strata;

import java.io.Serializable;

import com.goodworkalan.stash.Stash;

// TODO Document.
public final class LookupCooper<T, F extends Comparable<? super F>>
implements Cooper<T, F, T>, Serializable
{
    // TODO Document.
    private final static long serialVersionUID = 20070402L;

    // TODO Document.
    public T newBucket(Stash stash, Extractor<T, F> extract, T object)
    {
        return object;
    }

    // TODO Document.
    public F getFields(Stash stash, Extractor<T, F> extractor, T object)
    {
        return extractor.extract(stash, object);
    }

    // TODO Document.
    public T getObject(T object)
    {
        return object;
    }
    
    // TODO Document.
    public Cursor<T> wrap(Cursor<T> cursor)
    {
        return cursor;
    }
}