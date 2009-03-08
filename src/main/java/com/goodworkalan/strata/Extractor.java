package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
public interface Extractor<T, F extends Comparable<? super F>>
{
    // TODO Document.
    public F extract(Stash stash, T object);
}
