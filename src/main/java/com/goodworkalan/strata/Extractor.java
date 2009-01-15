package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

public interface Extractor<T, F extends Comparable<? super F>>
{
    public F extract(Stash stash, T object);
}